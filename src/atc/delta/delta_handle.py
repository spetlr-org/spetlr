from pyspark.sql import DataFrame

from atc.atc_exceptions import AtcException
from atc.config_master import TableConfigurator
from atc.functions import init_dbutils
from atc.spark import Spark


class DeltaHandleException(AtcException):
    pass


class DeltaHandleInvalidName(DeltaHandleException):
    pass


class DeltaHandleInvalidFormat(DeltaHandleException):
    pass


class DeltaHandle:
    def __init__(self, name: str, location: str = None, data_format: str = "delta"):
        self._name = name
        self._location = location
        self._data_format = data_format

        self._validate()

    @classmethod
    def from_tc(cls, id: str):
        tc = TableConfigurator()
        return cls(
            name=tc.table_name(id),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "delta"),
        )

    def _validate(self):
        """Validates that the name is either db.table or just table."""
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
