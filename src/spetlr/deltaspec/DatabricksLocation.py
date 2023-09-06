from dataclasses import dataclass
from typing import Union
from urllib.parse import urlparse


@dataclass
class TableName:
    table: str = None
    schema: str = None
    catalog: str = None

    @classmethod
    def from_str(cls, name: str = None) -> "TableName":
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

    def full_schema(self) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema}"
        else:
            return self.schema

    def __str__(self):
        return ".".join(p for p in [self.catalog, self.schema, self.table] if p)

    def to_level(self, n_parts: int = 3) -> "TableName":
        if n_parts == 0:
            return TableName()
        if n_parts == 1:
            return TableName(table=self.table)
        if n_parts == 2:
            return TableName(table=self.table, schema=self.schema)

        return TableName(table=self.table, schema=self.schema, catalog=self.catalog)


def standard_databricks_location(val: Union[str, bytes, None]) -> Union[str, None]:
    """In databricks, if no schema is given, then the scheme dbfs is used."""
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
