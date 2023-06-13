from datetime import date, datetime
from typing import Any, List, Optional, Union

from pyspark.sql import DataFrame

from spetlr.configurator.configurator import Configurator
from spetlr.exceptions import SpetlrException
from spetlr.functions import get_unique_tempview_name, init_dbutils
from spetlr.spark import Spark
from spetlr.tables.TableHandle import TableHandle
from spetlr.utils.CheckDfMerge import CheckDfMerge
from spetlr.utils.GetMergeStatement import GetMergeStatement


class DeltaHandleException(SpetlrException):
    pass


class DeltaHandleInvalidName(DeltaHandleException):
    pass


class DeltaHandleInvalidFormat(DeltaHandleException):
    pass


class DeltaHandle(TableHandle):
    def __init__(self, name: str, location: str = None, data_format: str = "delta"):
        self._name = name
        self._location = location
        self._data_format = data_format

        self._partitioning: Optional[List[str]] = None

        self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "DeltaHandle":
        tc = Configurator()
        return cls(
            name=tc.table_property(id, "name", ""),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "delta"),
        )

    def _validate(self):
        """Validates that the name is either db.table or just table."""
        if not self._name:
            if not self._location:
                raise DeltaHandleInvalidName(
                    "Cannot create DeltaHandle without name or path"
                )
            self._name = f"delta.`{self._location}`"
        else:
            name_parts = self._name.split(".")
            if len(name_parts) == 1:
                self._db = None
                self._table_name = name_parts[0]
            elif len(name_parts) == 2:
                self._db = name_parts[0]
                self._table_name = name_parts[1]
            else:
                raise DeltaHandleInvalidName(f"Could not parse name {self._name}")

        # only format delta is supported.
        if self._data_format != "delta":
            raise DeltaHandleInvalidFormat("Only format delta is supported.")

    def read(self) -> DataFrame:
        """Read table by path if location is given, otherwise from name."""
        if self._location:
            return Spark.get().read.format(self._data_format).load(self._location)
        return Spark.get().table(self._name)

    def write_or_append(
        self, df: DataFrame, mode: str, mergeSchema: bool = None
    ) -> None:
        assert mode in {"append", "overwrite"}

        writer = df.write.format(self._data_format).mode(mode)
        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if self._location:
            return writer.save(self._location)

        return writer.saveAsTable(self._name)

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "overwrite", mergeSchema=mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append", mergeSchema=mergeSchema)

    def truncate(self) -> None:
        Spark.get().sql(f"TRUNCATE TABLE {self._name};")

    def drop(self) -> None:
        Spark.get().sql(f"DROP TABLE IF EXISTS {self._name};")

    def drop_and_delete(self) -> None:
        self.drop()
        if self._location:
            init_dbutils().fs.rm(self._location, True)

    def create_hive_table(self) -> None:
        sql = f"CREATE TABLE IF NOT EXISTS {self._name} "
        if self._location:
            sql += f" USING DELTA LOCATION '{self._location}'"
        Spark.get().sql(sql)

    def recreate_hive_table(self):
        self.drop()
        self.create_hive_table()

    def get_partitioning(self):
        """The result of DESCRIBE DETAIL tablename is like this:
        +------+--------------------+--------------------+----------------+-------+
        |format|                  id|                name|partitionColumns|  ...  |
        +------+--------------------+--------------------+----------------+-------+
        | delta|c96a1e94-314b-427...|spark_catalog.tes...|    [colB, colA]|  ...  |
        +------+--------------------+--------------------+----------------+-------+
        but this method return the partitioning in the form ['mycolA'],
        if there is no partitioning, an empty list is returned.
        """
        if self._partitioning is None:
            self._partitioning = (
                Spark.get()
                .sql(f"DESCRIBE DETAIL {self.get_tablename()}")
                .select("partitionColumns")
                .collect()[0][0]
            )
        return self._partitioning

    def get_tablename(self) -> str:
        return self._name

    def upsert(
        self,
        df: DataFrame,
        join_cols: List[str],
    ) -> Union[DataFrame, None]:
        if df is None:
            return None

        df = df.filter(" AND ".join(f"({col} is NOT NULL)" for col in join_cols))
        print(
            "Rows with NULL join keys found in input dataframe"
            " will be discarded before load."
        )

        df_target = self.read()

        # If the target is empty, always do faster full load
        if len(df_target.take(1)) == 0:
            return self.write_or_append(df, mode="overwrite")

        # Find records that need to be updated in the target (happens seldom)

        # Define the column to be used for checking for new rows
        # Checking the null-ness of one right row is sufficient to mark the row as new,
        # since null keys are disallowed.

        df, merge_required = CheckDfMerge(
            df=df,
            df_target=df_target,
            join_cols=join_cols,
            avoid_cols=[],
        )

        if not merge_required:
            return self.write_or_append(df, mode="append")

        temp_view_name = get_unique_tempview_name()
        df.createOrReplaceTempView(temp_view_name)

        target_table_name = self.get_tablename()
        non_join_cols = [col for col in df.columns if col not in join_cols]

        merge_sql_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=target_table_name,
            source_table_name=temp_view_name,
            join_cols=join_cols,
            insert_cols=df.columns,
            update_cols=non_join_cols,
            special_update_set="",
        )

        df._jdf.sparkSession().sql(merge_sql_statement)

        print("Incremental Base - incremental load with merge")

        return df

    def delete_data(
        self, comparison_col: str, comparison_limit: Any, comparison_operator: str
    ) -> None:
        needs_quotes = (
            isinstance(comparison_limit, str)
            or isinstance(comparison_limit, date)
            or isinstance(comparison_limit, datetime)
        )
        limit = f"'{comparison_limit}'" if needs_quotes else comparison_limit
        sql_str = (
            f"DELETE FROM {self._name}"
            f" WHERE {comparison_col} {comparison_operator} {limit};"
        )
        Spark.get().sql(sql_str)
