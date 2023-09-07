"""This file contains helper classes and functions that are shared
by the DeltaTableSpec and the DeltaDatabaseSpec.
"""
from dataclasses import dataclass
from typing import Union
from urllib.parse import urlparse


@dataclass
class TableName:
    """The Table Name class gives access to the different parts of a table name
    Which are catalog.database.table. Where the first two are optional.
    """

    table: str = None
    schema: str = None
    catalog: str = None

    @classmethod
    def from_str(cls, name: str = None) -> "TableName":
        """
        Build the TableName object based on
        "catalog.database.table", "database.table" or just "table"
        """
        if not name:
            return cls()
        parts = name.split(".")
        table = parts.pop()
        if not parts:
            return cls(table=table)
        schema = parts.pop()
        if not parts:
            return cls(table=table, schema=schema)
        catalog = parts.pop()
        return cls(table=table, schema=schema, catalog=catalog)

    def __str__(self):
        """
        Get the string representing this the TableName object like
        "catalog.database.table", "database.table" or just "table"
        """
        return ".".join(p for p in [self.catalog, self.schema, self.table] if p)

    def to_level(self, n_parts: int = 3) -> "TableName":
        """
        Get the TableName object but only specified to the given
        number of levels up to 3
        """
        if n_parts == 0:
            return TableName()
        if n_parts == 1:
            return TableName(table=self.table)
        if n_parts == 2:
            return TableName(table=self.table, schema=self.schema)

        return TableName(table=self.table, schema=self.schema, catalog=self.catalog)


def standard_databricks_location(val: Union[str, bytes, None]) -> Union[str, None]:
    """In databricks, if no scheme is given, then the scheme dbfs is used."""
    if val is None:
        return None
    val = ensureStr(val)
    p = urlparse(val)
    if not p.scheme:
        p = p._replace(scheme="dbfs")

    return p.geturl()


def ensureStr(input: Union[str, bytes]) -> str:
    """Takes string or bytes and always returns a string."""
    try:
        return input.decode()
    except (UnicodeDecodeError, AttributeError):
        return input
