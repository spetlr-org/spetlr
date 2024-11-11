from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pyspark.sql.types as T
from pyspark.sql import DataFrame

from spetlr.configurator.configurator import Configurator
from spetlr.exceptions import SpetlrException
from spetlr.functions import get_unique_tempview_name, init_dbutils
from spetlr.schema_manager import SchemaManager
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
    def __init__(
        self,
        name: str = None,
        location: str = None,
        schema: T.StructType = None,
        data_format: str = "delta",
        options_dict: Dict[str, str] = None,
        ignore_changes: bool = True,
        stream_start: Union[datetime, str] = None,
        max_bytes_per_trigger: int = None,
    ):
        """
        name: The name of the Delta table, can be omitted if location is given.
        location (optional): The file-system path to the Delta table files.
        data_format (optional): Always delta-format. Todo: Remove in future PR.
        options_dict (optional): All other string options for pyspark.
        ignore_changes (optional): ignore transactions that delete data
                                    at partition boundaries.
        stream_start (optional):  If string format, it accepts anything that
                                  the `dateparser` library can parse.
        max_bytes_per_trigger (optional): How much data gets
                                processed in each micro-batch.
        """
        if name is None:
            if location is None:
                raise ValueError("`name`  or `location` must be given")
            name = f"delta.`{location}`"

        self._name = name
        self._location = location
        self._schema = schema
        self._data_format = data_format

        self._partitioning: Optional[List[str]] = None
        self._cluster: Optional[List[str]] = None
        self._validate()

        if options_dict is None or options_dict == "":
            self.set_options_dict({})
        else:
            self.set_options_dict(options_dict)

        self._options_dict.update({"ignoreChanges": str(ignore_changes)})

        if stream_start and stream_start != "":
            if isinstance(stream_start, datetime):
                self._options_dict["startingTimestamp"] = stream_start.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            else:
                self._options_dict["startingTimestamp"] = stream_start

        if max_bytes_per_trigger and max_bytes_per_trigger != "":
            self._options_dict["maxBytesPerTrigger"] = str(max_bytes_per_trigger)

    @classmethod
    def from_tc(cls, id: str) -> "DeltaHandle":
        tc = Configurator()
        return cls(
            name=tc.get(id, "name", ""),
            location=tc.get(id, "path", ""),
            schema=SchemaManager().get_schema(id, None),
            data_format=tc.get(id, "format", "delta"),
            ignore_changes=tc.get(id, "ignore_changes", "True"),
            stream_start=tc.get(id, "stream_start", ""),
            max_bytes_per_trigger=tc.get(id, "max_bytes_per_trigger", ""),
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
            elif len(name_parts) == 3:
                self._catalog = name_parts[0]
                self._db = name_parts[1]
                self._table_name = name_parts[2]
            else:
                raise DeltaHandleInvalidName(f"Could not parse name {self._name}")

        # only format delta is supported.
        if self._data_format != "delta":
            raise DeltaHandleInvalidFormat("Only format delta is supported.")

    def read(self) -> DataFrame:
        """If name is available, always read the table by name."""
        if not self._location or (self._name and "." in self._name):
            return Spark.get().table(self._name)
        return Spark.get().read.format(self._data_format).load(self._location)

    def write_or_append(
        self,
        df: DataFrame,
        mode: str,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
        *,
        overwritePartitions: bool = None,
    ) -> None:
        assert mode in {"append", "overwrite"}

        writer = df.write.format(self._data_format).mode(mode)
        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if overwriteSchema is not None:
            writer = writer.option(
                "overwriteSchema", "true" if overwriteSchema else "false"
            )

        if overwritePartitions:
            writer = writer.option("partitionOverwriteMode", "dynamic")

        return writer.saveAsTable(self._name)

    def overwrite(
        self,
        df: DataFrame,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
        *,
        overwritePartitions: bool = None,
    ) -> None:
        return self.write_or_append(
            df,
            "overwrite",
            mergeSchema=mergeSchema,
            overwriteSchema=overwriteSchema,
            overwritePartitions=overwritePartitions,
        )

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

    def get_cluster(self):
        """The result of DESCRIBE DETAIL tablename is like this:
        +------+--------------------+--------------------+----------------+-------+
        |format|                  id|                name|clusteringColumns|  ...  |
        +------+--------------------+--------------------+----------------+-------+
        | delta|c96a1e94-314b-427...|spark_catalog.tes...|    [colB, colA]|  ...  |
        +------+--------------------+--------------------+----------------+-------+
        but this method return the cluster in the form ['mycolA'],
        if there is no cluster, an empty list is returned.
        """
        if self._cluster is None:
            self._cluster = (
                Spark.get()
                .sql(f"DESCRIBE DETAIL {self.get_tablename()}")
                .select("clusteringColumns")
                .collect()[0][0]
            )
        return self._cluster

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
        df.createOrReplaceGlobalTempView(temp_view_name)

        target_table_name = self.get_tablename()
        non_join_cols = [col for col in df.columns if col not in join_cols]

        merge_sql_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=target_table_name,
            source_table_name="global_temp." + temp_view_name,
            join_cols=join_cols,
            insert_cols=df.columns,
            update_cols=non_join_cols,
            special_update_set="",
        )

        Spark.get().sql(merge_sql_statement)

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

    def read_stream(self) -> DataFrame:
        return (
            Spark.get()
            .readStream.format(self._data_format)
            .options(**self._options_dict)
            .table(self._name)
        )

    def set_options_dict(self, options: Dict[str, str]):
        self._options_dict = options

    def get_schema(self) -> T.StructType:
        return self._schema

    def set_schema(self, schema: T.StructType) -> "DeltaHandle":
        self._schema = schema

        return self
