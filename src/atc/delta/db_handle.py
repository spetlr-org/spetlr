from atc.configurator.configurator import Configurator
from atc.exceptions import AtcException
from atc.spark import Spark


class DbHandleException(AtcException):
    pass


class DbHandleInvalidName(DbHandleException):
    pass


class DbHandleInvalidFormat(DbHandleException):
    pass


class DbHandle:
    def __init__(self, name: str, location: str = None, data_format: str = "db"):
        self._name = name
        self._location = location
        self._data_format = data_format

        self._validate()

    @classmethod
    def from_tc(cls, id: str):
        tc = Configurator()
        return cls(
            name=tc.table_name(id),
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", "db"),
        )

    def _validate(self):
        # name is either `db`.`table` or just `table`
        if "." in self._name:
            raise DbHandleInvalidName(f"Invalid DB name {self._name}")

        # only format db is supported.
        if self._data_format != "db":
            raise DbHandleInvalidFormat("Format must be db or null.")

    def drop(self) -> None:
        Spark.get().sql(f"DROP DATABASE IF EXISTS {self._name};")

    def drop_cascade(self) -> None:
        Spark.get().sql(f"DROP DATABASE IF EXISTS {self._name} CASCADE;")

    def create(self) -> None:
        sql = f"CREATE DATABASE IF NOT EXISTS {self._name} "
        if self._location:
            sql += f" LOCATION '{self._location}'"
        Spark.get().sql(sql)
