import copy
import json
from dataclasses import dataclass
from typing import Dict, Optional

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.deltaspec.DatabricksLocation import standard_databricks_location
from spetlr.deltaspec.exceptions import InvalidSpecificationError
from spetlr.exceptions import NoSuchValueException
from spetlr.spark import Spark


@dataclass
class DeltaDatabaseSpec:
    name: str
    comment: Optional[str] = None
    location: Optional[str] = None
    dbproperties: Dict[str, str] = None

    def __init__(
        self,
        name: str,
        comment: Optional[str] = None,
        location: Optional[str] = None,
        dbproperties: Dict[str, str] = None,
    ):
        self.name = name
        self.comment = comment
        self.location = standard_databricks_location(location)
        self.dbproperties = dbproperties

    def __repr__(self):
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
    def from_tc(cls, id: str):
        """Build a DbSpec instance from what is in the Configurator.
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
        details = parse_single_sql_statement(sql)
        if details.get("format").lower() != "db":
            raise InvalidSpecificationError(
                "The sql code is not a create database statement."
            )

        init_args = dict(
            name=details.get("name"),
            location=details.get("path"),
            comment=details.get("comment"),
            dbproperties=details.get("dbproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}
        return DeltaDatabaseSpec(**init_args)

    def get_create_sql(self):
        name_part = f"CREATE SCHEMA IF NOT EXISTS {self.name}"
        comment_part = f"  COMMENT={json.dumps(self.comment)}" if self.comment else ""
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
    def from_spark(cls, name: str):
        rows = Spark.get().sql(f"DESCRIBE SCHEMA {name}").collect()
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
        result = copy.copy(self)

        c = Configurator()
        details = c.get_all_details()
        result.name = self.name.format(**details)
        result.location = self.location.format(**details)

        return result
