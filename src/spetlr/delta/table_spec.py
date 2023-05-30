"""
Specifications are based on this documentation:
https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using
"""
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from spetlr import Configurator
from spetlr.exceptions import NoSuchValueException
from spetlr.spark import Spark


class InvalidSpecificationError(Exception):
    """Internally this exception is used when attempting
    to unpack configurations into table and db specifications"""


@dataclass
class ColSpec:
    """This class represents a complete description of a column
    as used in databricks tables"""

    name: str
    type_: str = None
    nullable: Optional[bool] = None
    comment: Optional[str] = None

    # TODO: generated and column_constraint
    def __repr__(self):
        return (
            f'ColSpec(name="{self.name}", '
            + (f'type_="{self.type_}", ' if self.type_ else "")
            + (f"nullable={self.nullable}, " if self.nullable is not None else "")
            + (f"comment={repr(self.comment)}, " if self.comment else "")
            + ")"
        )


@dataclass
class DbSpec:
    name: str
    comment: Optional[str] = None
    location: Optional[str] = None
    dbproperties: Dict[str, str] = None

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
                    f"DbSpec(name={repr(self.name)}",
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
            data_source = c.get(id, "format")
        except NoSuchValueException:
            raise InvalidSpecificationError()

        if data_source not in ["db", "schema"]:
            raise InvalidSpecificationError()

        location = c.get(id, "path", default=None)
        comment = c.get(id, "comment", default=None)
        dbproperties = c.get(id, "dbproperties", default=None)

        return cls(
            name=name, location=location, comment=comment, dbproperties=dbproperties
        )

    def get_create_sql(self):
        name_part = f"CREATE SCHEMA {self.name}"
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


@dataclass
class CatalogSpec:
    dbs: Dict[str, DbSpec] = field(default_factory=dict)


@dataclass
class TableSpec:
    """This class represents a full specification for a delta table."""

    table_name: str
    data_source: str = None
    columns: List[ColSpec] = field(default_factory=list)
    options: Dict[str, str] = field(default_factory=dict)
    partitioned_by: List[ColSpec] = field(default_factory=list)
    tblproperties: Dict[str, str] = field(default_factory=dict)
    location: Optional[str] = None
    # TODO: Clustered By

    def __repr__(self):
        column_part = ""
        if self.columns:
            description = ", ".join(repr(col) for col in self.columns)
            column_part = f"columns=[{description}], "

        data_source_part = (
            f'data_source="{self.data_source}", ' if self.data_source else ""
        )

        options_part = ""
        if self.options:
            description = ", ".join(f'"{k}":"{v}"' for k, v in self.options.items())
            options_part = f"options={{{description}}}, "

        partitioned_by_part = ""
        if self.partitioned_by:
            description = ", ".join(repr(col) for col in self.partitioned_by)
            partitioned_by_part = f"partitioned_by=[{description}], "

        tblproperties_part = ""
        if self.tblproperties:
            description = ", ".join(
                f'"{k}":"{v}"' for k, v in self.tblproperties.items()
            )
            tblproperties_part = f"tblproperties={{{description}}}, "

        location_part = f'location="{self.location}", ' if self.location else ""

        return (
            f'TableSpec(table_name="{self.table_name}",'
            + column_part
            + data_source_part
            + options_part
            + partitioned_by_part
            + tblproperties_part
            + location_part
            + ")"
        )
