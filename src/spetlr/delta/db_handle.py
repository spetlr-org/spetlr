import json
from dataclasses import dataclass
from typing import List, Optional

from spetlr.configurator.configurator import Configurator
from spetlr.exceptions import SpetlrException
from spetlr.spark import Spark


class DbHandleException(SpetlrException):
    pass


class DbHandleInvalidName(DbHandleException):
    pass


class DbHandleInvalidFormat(DbHandleException):
    pass


@dataclass
class DbHandle:
    """A DbHandle contains all identifying information
    of a delta database (aka schema). The dataclass nature allows
    comparisons between different DbHandle instances, based on their properties."""

    name: str
    comment: Optional[str] = None
    location: Optional[str] = None

    def __init__(
        self,
        name: str,
        location: str = None,
        data_format: str = "db",
        comment: str = None,
    ):
        self.name = name
        self.location = location
        self.comment = comment

        self._data_format = data_format

        self._validate()

    @classmethod
    def from_tc(cls, id: str):
        """Construct a Database(aka Schema) object based on the configurator id."""
        c = Configurator()
        name = c.get(id, "name")
        location = c.get(id, "path", default=None)
        comment = c.get(id, "comment", default=None)
        data_format = c.get(id, "format", "db")

        return cls(
            name=name, location=location, comment=comment, data_format=data_format
        )

    @classmethod
    def from_spark(cls, name: str):
        """Construct a Database(aka Schema) object based hive data."""
        rows = Spark.get().sql(f"DESCRIBE SCHEMA {name}").collect()
        comment = location = None
        for row in rows:
            if str(row[0]).lower() == "comment":
                comment = str(row[1])
            elif str(row[0]).lower() == "location":
                location = str(row[1])

        # TODO: dbproperties currently not supported

        return cls(name=name, location=location, comment=comment, data_format="db")

    def get_create_sql(self):
        name_part = f"CREATE DATABASE {self.name} IF NOT EXISTS"
        comment_part = f"  COMMENT={json.dumps(self.comment)}" if self.comment else ""
        location_part = (
            f"  LOCATION {json.dumps(self.location)}" if self.location else ""
        )
        return "\n".join(
            part for part in [name_part, comment_part, location_part] if part
        )

    def exists(self):
        """Does this database exist in the hive metastore?"""
        return bool(Spark.get().sql(f'show schemas like "{self.name}"').count())

    def matches_spark(self):
        """Does the hive metastore match this specification?"""
        db = DbHandle.from_spark(self.name)
        return self == db

    def _validate(self):
        # name is either `db`.`table` or just `table`
        if "." in self.name:
            raise DbHandleInvalidName(f"Invalid DB name {self.name}")

        # only format db is supported.
        if self._data_format != "db":
            raise DbHandleInvalidFormat("Format must be db or null.")

    def drop(self) -> None:
        Spark.get().sql(f"DROP DATABASE IF EXISTS {self.name};")

    def drop_cascade(self) -> None:
        Spark.get().sql(f"DROP DATABASE IF EXISTS {self.name} CASCADE;")

    def getTables(self) -> List:
        """Return the list of DeltaHandle
        representing all the tables in this database"""
        from spetlr.delta import DeltaHandle

        return [
            DeltaHandle.from_spark(f"{self.name}.{tbl}")
            for (tbl,) in Spark.get()
            .sql(f"SHOW TABLES FROM {self.name}")
            .select("tableName")
            .collect()
        ]

    def create(self) -> None:
        Spark.get().sql(self.get_create_sql())

    def __repr__(self):
        """A full python constructor to regenerate this object."""
        return (
            ", ".join(
                part
                for part in [
                    f"DbHandle(name={repr(self.name)}",
                    (f"comment={repr(self.comment)}" if self.comment else ""),
                    (f"location={repr(self.location)}" if self.location else ""),
                ]
                if part
            )
            + ")"
        )
