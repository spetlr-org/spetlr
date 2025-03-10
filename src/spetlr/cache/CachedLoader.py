import sys
import time
from functools import wraps
from typing import List, Optional

import pyspark.sql.functions as f
from exceptiongroup import ExceptionGroup
from pyspark.sql import DataFrame

from spetlr.cache.CachedLoaderParameters import CachedLoaderParameters
from spetlr.etl import Loader
from spetlr.exceptions import SpetlrKeyError
from spetlr.functions import get_unique_tempview_name
from spetlr.spark import Spark


def _retry_cache(func):
    @wraps(func)
    def wrapper(self: "CachedLoader", *args, **kwargs):
        if not self.params.retry_cache_writes:
            return func(self, *args, **kwargs)
        else:
            exceptions = []
            for _ in range(0, self.params.retry_cache_writes + 1):
                try:
                    result = func(self, *args, **kwargs)
                    return result
                except Exception as e:
                    exceptions.append(e)
                time.sleep(self.params.retry_cache_wait_seconds)
            raise ExceptionGroup(
                f"The function {func} failed even "
                f"after {self.params.retry_cache_writes} retries",
                exceptions,
            )

    return wrapper


class CachedLoader(Loader):
    """
    This class is an ETL loader that uses a cache table to turn
    a full load of a dataframe into an incremental load, where only new
    or changed rows are passed on. Deleted rows are also passed on but
    to a different method.
    The cache table needs to exist and have the correct schema matching
    the configured parameters, see the CachedLoaderParameters class.

    Remember to override self.write_operation and/or self.delete_operation.
    If the parameter "do_nothing_if_more_rows_than" was specified,
       you also need to override self.too_many_rows.
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
        too_many_rows_not_overloaded = "too_many_rows" not in self.__class__.__dict__
        if write_not_overloaded or delete_not_overloaded:
            raise AssertionError(
                "write_operation and delete_operation methods required"
            )
        if too_many_rows_not_overloaded and p.do_nothing_if_more_rows_than is not None:
            raise AssertionError("too_many_rows method required")

        if self.__class__ is CachedLoader:
            raise AssertionError("You should inherit from this class")

    def write_operation(self, df: DataFrame) -> Optional[DataFrame]:
        """Abstract Method to be overridden in child."""
        raise NotImplementedError()

    def delete_operation(self, df: DataFrame) -> Optional[DataFrame]:
        """Abstract Method to be overridden in child."""
        raise NotImplementedError()

    def too_many_rows(self) -> None:
        """Abstract Method to be overridden in child."""
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        in_cols = df.columns

        if not set(self.params.key_cols).issubset(in_cols):
            raise AssertionError(
                "The key columns must be given in the input dataframe."
            )

        df = self._discard_null_keys(df)

        # ensure that no duplicates exist. The keys have to be a unique set.
        df = df.dropDuplicates(self.params.key_cols)

        cache = self._extract_cache()

        result = self._discard_non_new_rows_against_cache(df, cache)

        if self.params.do_nothing_if_more_rows_than is not None:
            if (
                result.to_be_written.count() + result.to_be_deleted.count()
            ) > self.params.do_nothing_if_more_rows_than:
                self.too_many_rows()
                return

        # write branch
        if self.params.provisional_markup_step:
            pre_write_version = self._perform_provisional_markup(result.to_be_written)
            df_written = self.write_operation(result.to_be_written)
            # no context manager or try-catch loop is used here. If the operation
            # fails, we want the provisional markup to remain, ensuring correct data
            # updates on next run.
            self._rollback_provisional_markup(pre_write_version)
        else:
            df_written = self.write_operation(result.to_be_written)

        if df_written:
            # this line only executes if the line above does not raise an exception.
            df_written_cache_update = self._prepare_written_cache_update(
                df_written, in_cols
            )
            self._load_cache(df_written_cache_update)

        # delete branch
        if self.params.provisional_markup_step:
            pre_delete_versions = self._perform_provisional_markup(result.to_be_deleted)
            df_deleted = self.delete_operation(result.to_be_deleted)
            # no context manager or try-catch loop is used here. If the operation
            # fails, we want the provisional markup to remain, ensuring correct data
            # updates on next run.
            self._rollback_provisional_markup(pre_delete_versions)
        else:
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

    @_retry_cache
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

    def _discard_null_keys(self, df: DataFrame) -> DataFrame:
        null_df = df
        for col in self.params.key_cols:
            null_df = null_df.filter(f.col(col).isNull())
        if null_df.count():
            raise SpetlrKeyError("The incoming dataframe contains null keys")
        return df

    @_retry_cache
    def _perform_provisional_markup(self, df: DataFrame) -> int:
        """The cache table is updated with a provisional update where all
        *potentially* affected rows have their hash set to zero.
        If the following operation succeeds, the provisional markup is rolled
        back and the *actually* affected rows have their correct markup applied.
        If the operation fails, however, no further corrective action is required
        to ensure a full re-write of the row in the target system.

        The function returns the version that should be restored if all goes to
        plan."""
        spark = Spark().get()
        p = self.params

        pre_version = (
            Spark.get()
            .sql(f"DESCRIBE HISTORY {p.cache_table_name} LIMIT 1")
            .select("version")
            .take(1)[0][0]
        )

        (df.select(*p.key_cols).createOrReplaceTempView("provisionalMarkupKeys"))
        other_cols = [p.deletedTime] + p.cache_id_cols
        spark.sql(
            f"""
                MERGE INTO {p.cache_table_name} AS c
                USING provisionalMarkupKeys AS p
                ON  {' AND '.join(
                f'(c.{col} = p.{col})' for col in p.key_cols
            )}
                WHEN MATCHED THEN
                    UPDATE SET {p.rowHash} = 0
                WHEN NOT MATCHED THEN
                    INSERT ( {', '.join(p.key_cols)},
                            {p.rowHash}, {p.loadedTime},
                            {', '.join(other_cols)})
                    VALUES ( {', '.join(f'p.{c}' for c in p.key_cols)},
                            0, current_timestamp(),
                            {', '.join('NULL' for _ in other_cols)})
            """
        )
        return pre_version

    @_retry_cache
    def _rollback_provisional_markup(self, version: int) -> None:
        Spark.get().sql(
            f"RESTORE TABLE {self.params.cache_table_name} "
            f"TO VERSION AS OF {version}"
        )
