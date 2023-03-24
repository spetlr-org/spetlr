import sys
from typing import List, Optional

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from atc.etl import Loader
from atc.spark import Spark

from ..functions import get_unique_tempview_name
from .CachedLoaderParameters import CachedLoaderParameters


class CachedLoader(Loader):
    """
    This class is an ETL loader that uses a cache table to turn
    a full load of a dataframe into an incremental load, where only new
    or changed rows are passed on. Deleted rows are also passed on but
    to a different method.
    The cache table needs to exist and have the correct schema matching
    the configured parameters, see the CachedLoaderParameters class.

    Remember to override self.write_operation and/or self.delete_operation
    Any variable rows need to be added in this function.
    """

    class ReductionResult:
        to_be_written: DataFrame
        to_be_deleted: DataFrame

    def __init__(self, params: CachedLoaderParameters):
        super().__init__()
        self.params = params
        self.validate()

    def validate(self):
        # validate cache table schema
        p = self.params
        df = Spark.get().table(p.cache_table_name)
        try:
            assert set(df.columns) == set(
                p.key_cols + p.cache_id_cols + [p.rowHash, p.loadedTime, p.deletedTime]
            )
        except AssertionError:
            print(
                "ERROR: The cache table needs to have precisely the correct schema."
                " Found:",
                df.columns,
                file=sys.stderr,
            )
            raise

        # validate overloading
        write_not_overloaded = "write_operation" not in self.__class__.__dict__
        delete_not_overloaded = "delete_operation" not in self.__class__.__dict__
        if write_not_overloaded or delete_not_overloaded:
            raise AssertionError("write_operation and delete_operation required")

        if self.__class__ is CachedLoader:
            raise AssertionError("You should inherit from this class")

    def write_operation(self, df: DataFrame) -> Optional[DataFrame]:
        """Abstract Method to be overridden in child."""
        raise NotImplementedError()

    def delete_operation(self, df: DataFrame) -> Optional[DataFrame]:
        """Abstract Method to be overridden in child."""
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        in_cols = df.columns

        if not set(self.params.key_cols).issubset(in_cols):
            raise AssertionError(
                "The key columns must be given in the input dataframe."
            )

        # ensure that no duplicates exist. The keys have to be a unique set.
        df = df.dropDuplicates(self.params.key_cols)

        cache = self._extract_cache()

        result = self._discard_non_new_rows_against_cache(df, cache)

        # write branch
        df_written = self.write_operation(result.to_be_written)
        if df_written:
            # this line only executes if the line above does not raise an exception.
            df_written_cache_update = self._prepare_written_cache_update(
                df_written, in_cols
            )
            self._load_cache(df_written_cache_update)

        # delete branch
        df_deleted = self.delete_operation(result.to_be_deleted)
        if df_deleted:
            df_deleted_cache_update = self._prepare_deleted_cache_update(df_deleted)

            self._load_cache(df_deleted_cache_update)
            # this method is called a again separately in order to ensure that the
            # written cache is saved even if the delete operation fails.

        return

    def _extract_cache(self) -> DataFrame:
        # here we fix the version,
        # so we don't overwrite the cache before we might want to use it.
        version = (
            Spark.get()
            .sql(f"DESCRIBE HISTORY {self.params.cache_table_name} LIMIT 1")
            .select("version")
            .take(1)[0][0]
        )
        cache = Spark.get().sql(
            f"SELECT * FROM {self.params.cache_table_name} VERSION AS OF {version}"
            f" WHERE {self.params.deletedTime} IS NULL"
            f" AND {self.params.rowHash} IS NOT NULL"
            f" AND {self.params.loadedTime} IS NOT NULL"
        )

        return cache

    def _prepare_written_cache_update(self, df: DataFrame, columns_to_hash: List[str]):
        # re-hash the input row
        return df.select(
            *self.params.key_cols,
            f.hash(*columns_to_hash).alias(self.params.rowHash),
            f.current_timestamp().alias(self.params.loadedTime),
            f.lit(None).cast("timestamp").alias(self.params.deletedTime),
            *self.params.cache_id_cols,
        )

    def _prepare_deleted_cache_update(self, df: DataFrame):
        return df.select(
            *self.params.key_cols,
            self.params.rowHash,
            self.params.loadedTime,
            f.current_timestamp().alias(self.params.deletedTime),
            *self.params.cache_id_cols,
        )

    def _load_cache(self, cache_to_load: DataFrame) -> None:
        view_name = get_unique_tempview_name()

        merge_sql_statement = (
            f"MERGE INTO {self.params.cache_table_name} AS target "
            f"USING {view_name} as source "
            f"ON "
            + (
                " AND ".join(
                    f"(source.{col} = target.{col})" for col in self.params.key_cols
                )
            )
            +
            # update existing records
            "WHEN MATCHED THEN UPDATE SET * "
            # insert new records.
            "WHEN NOT MATCHED THEN INSERT * "
        )
        cache_to_load.createOrReplaceTempView(view_name)
        Spark.get().sql(merge_sql_statement)

    def _discard_non_new_rows_against_cache(
        self, df_in: DataFrame, cache: DataFrame
    ) -> ReductionResult:
        """
        Returns:
            to_be_written contains only rows that
             - are new in the cache, or
             - are a mismatch against the cache
            to_be_deleted contains rows that
             - are present in the cache but not in the data
        """
        in_cols = df_in.columns

        # ensure no null keys:
        df_in = df_in.filter(
            " AND ".join(f"({col} is NOT NULL)" for col in self.params.key_cols)
        )

        # prepare hash of row
        df_hashed = df_in.withColumn("rowHash", f.hash("*"))

        # add a column to distinguish rows after the join
        df_hashed = df_hashed.withColumn("fromPayload", f.lit(True))

        # join with cache
        joined_df = (
            df_hashed.alias("df")
            .join(cache.alias("cache"), self.params.key_cols, "full")
            .cache()
        )

        result = self.ReductionResult()

        # rows coming from the original df will have a non-null fromPayload column
        result.to_be_written = (
            joined_df.filter("fromPayload IS NOT NULL")
            .filter(
                # either the row has never been loaded before
                (f.col(f"cache.{self.params.loadedTime}").isNull())
                # or it has changed wrt the previous load
                | (
                    f.col(f"df.{self.params.rowHash}")
                    != f.col(f"cache.{self.params.rowHash}")
                )
            )
            .select(
                *self.params.key_cols,
                *[f"df.{c}" for c in in_cols if c not in self.params.key_cols],
            )
        )

        # cached rows with no incoming match will have a null fromPayload column
        if Spark.version() > Spark.DATABRICKS_RUNTIME_9_1:
            # from databricks 10.4 this returns all columns, including key columns.
            # specifying key columns again causes them to be present twice in the
            # data frame
            result.to_be_deleted = joined_df.filter("fromPayload IS NULL").select(
                "cache.*"
            )
        else:
            # up to databricks 9.1 the key columns need to be selected separately,
            # since they are not returned by the cache.*
            result.to_be_deleted = joined_df.filter("fromPayload IS NULL").select(
                *self.params.key_cols, "cache.*"
            )

        return result
