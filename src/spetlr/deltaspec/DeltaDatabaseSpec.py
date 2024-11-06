import json
from dataclasses import asdict, dataclass
from typing import Dict, Optional

from deprecated import deprecated
from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.deltaspec.exceptions import InvalidSpecificationError
from spetlr.deltaspec.helpers import ensureStr, standard_databricks_location
from spetlr.exceptions import NoSuchValueException
from spetlr.spark import Spark

# TODO: for writing remove checks on comment equality


@deprecated("Class untested in current version of spetlr")
@dataclass
class DeltaDatabaseSpec:
    name: str
    comment: Optional[str] = None
    location: Optional[str] = None
    dbproperties: Dict[str, str] = None

    def __post_init__(self):
        self.name = ensureStr(self.name)
        self.location = standard_databricks_location(self.location)

    def __repr__(self):
        """returns python code that can construct the present object such as
        DeltaDatabaseSpec(
            name="mydb_name",
            comment="cool data",
            location="dbfs:/my/path"
        )
        """
        dbproperties_part = ""
        if self.dbproperties:
            description = ", ".join(
                f'"{k}":"{v}"' for k, v in self.dbproperties.items()
            )
            dbproperties_part = f"dbproperties={{{description}}}, "

        return (
            ", ".join(
                part
                for part in [
                    f"DeltaDatabaseSpec(name={repr(self.name)}",
                    (f"comment={repr(self.comment)}" if self.comment else ""),
                    (f"location={repr(self.location)}" if self.location else ""),
                    dbproperties_part,
                ]
                if part
            )
            + ")"
        )

    @classmethod
    def from_tc(cls, id: str) -> "DeltaDatabaseSpec":
        """Build a DeltaDatabaseSpec instance from what is in the Configurator.
        This may have previously been parsed from sql."""
        c = Configurator()
        try:
            name = c.get(id, "name")
        except NoSuchValueException:
            raise InvalidSpecificationError()

        location = c.get(id, "path", default=None)
        comment = c.get(id, "comment", default=None)
        dbproperties = c.get(id, "dbproperties", default=None)

        return cls(
            name=name, location=location, comment=comment, dbproperties=dbproperties
        )

    @classmethod
    def from_sql(cls, sql: str) -> "DeltaDatabaseSpec":
        """Parse a sigle CREATE DATABASE statement
        to initialize a DeltaDatabaseSpec object."""
        details = parse_single_sql_statement(sql)
        format = details.get("format").lower()
        if format != "db":
            raise InvalidSpecificationError(
                "The sql code is not a create database statement."
                f"Instead, the provided format was {format}"
            )

        init_args = dict(
            name=details.get("name"),
            location=details.get("path"),
            comment=details.get("comment"),
            dbproperties=details.get("dbproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}
        return cls(**init_args)

    def get_create_sql(self) -> str:
        """Return the SQL code that will create the database that this
        class describes"""
        name_part = f"CREATE SCHEMA IF NOT EXISTS {self.name}"
        comment_part = f"  COMMENT {json.dumps(self.comment)}" if self.comment else ""
        location_part = (
            f"  LOCATION {json.dumps(self.location)}" if self.location else ""
        )
        dbproperties_part = ""
        if self.dbproperties:
            description = ", ".join(
                f"{k}={json.dumps(v)}" for k, v in self.dbproperties.items()
            )
            dbproperties_part = f"  WITH DBPROPERTIES ({description})"
        return "\n".join(
            part
            for part in [name_part, comment_part, location_part, dbproperties_part]
            if part
        )

    @classmethod
    def from_spark(cls, name: str) -> "DeltaDatabaseSpec":
        try:
            rows = Spark.get().sql(f"DESCRIBE SCHEMA {name}").collect()
        except AnalysisException:
            return None  # No such schema

        comment = location = None
        for row in rows:
            if str(row[0]).lower() == "comment":
                comment = str(row[1])
            elif str(row[0]).lower() == "location":
                location = str(row[1])

        # TODO: parsing of dbproperties currently not supported

        return cls(name=name, location=location, comment=comment)

    def fully_substituted(self) -> "DeltaDatabaseSpec":
        """Return a new DeltaDatabaseSpec
        where name and location have been completed via the Configurator."""
        parts = asdict(self)

        c = Configurator()
        details = c.get_all_details()
        parts["name"] = self.name.format(**details)
        parts["location"] = self.location.format(**details)

        return DeltaDatabaseSpec(**parts)

    def matches_to_spark(self) -> bool:
        """Check if the specification of this Database agrees
        with the specification of the database of the same name
        in the spark catalog."""
        full = self.fully_substituted()
        onstorage = self.from_spark(full.name)
        return full == onstorage

    def create(self):
        Spark.get().sql(self.get_create_sql())

    def drop_cascade(self):
        Spark.get().sql(f"DROP DATABASE {self.name} CASCADE")
