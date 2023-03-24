from typing import List, Optional, Union

from pyspark.sql import DataFrame

from atc.configurator.configurator import Configurator
from atc.exceptions import AtcException
from atc.functions import get_unique_tempview_name, init_dbutils
from atc.spark import Spark
from atc.tables.TableHandle import TableHandle
from atc.utils.CheckDfMerge import CheckDfMerge
from atc.utils.GetMergeStatement import GetMergeStatement


class DeltaHandleException(AtcException):
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
        """The result of DESCRIBE TABLE tablename is like this:
        +-----------------+---------------+-------+
        |         col_name|      data_type|comment|
        +-----------------+---------------+-------+
        |           mycolA|         string|       |
        |           myColB|            int|       |
        |                 |               |       |
        |   # Partitioning|               |       |
        |           Part 0|         mycolA|       |
        +-----------------+---------------+-------+
        but this method return the partitioning in the form ['mycolA'],
        if there is no partitioning, an empty list is returned.
        """
        if self._partitioning is None:
            # create an iterator object and use it in two steps
            rows_iter = iter(
                Spark.get().sql(f"DESCRIBE TABLE {self.get_tablename()}").collect()
            )

            # roll over the iterator until you see the title line
            for row in rows_iter:
                # discard rows until the important section header
                if row.col_name.strip() == "# Partitioning":
                    break
            # at this point, the iterator has moved past the section heading
            # leaving only the rows with "Part 1" etc.

            # create a list from the rest of the iterator like [(0,colA), (1,colB)]
            parts = [
                (int(row.col_name[5:]), row.data_type)
                for row in rows_iter
                if row.col_name.startswith("Part ")
            ]
            # sort, just in case the parts were out of order.
            parts.sort()

            # discard the index and put into an ordered list.
            self._partitioning = [p[1] for p in parts]
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

        Spark.get().sql(merge_sql_statement)

        print("Incremental Base - incremental load with merge")

        return df
