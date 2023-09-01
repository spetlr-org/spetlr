from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class TableName:
    table: str
    schema: str = None
    catalog: str = None

    @classmethod
    def from_str(cls, name: str) -> "TableName":
        parts = name.split(".")
        table = parts.pop()
        schema = parts.pop() if parts else None
        catalog = parts.pop() if parts else None
        return cls(table=table, schema=schema, catalog=catalog)

    def full_schema(self) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.schema}"
        else:
            return self.schema


def standard_databricks_location(val: str) -> str:
    p = urlparse(val)
    if not p.scheme:
        p = p._replace(scheme="dbfs")

    return p.geturl()
