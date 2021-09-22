from collections import namedtuple
from enum import Enum


class States(Enum):
    RELEASE = 0
    DEBUG = 1


TableAlias = namedtuple("TableAlias", [e.name.lower() for e in States])


class Details:
    def __init__(
        self,
        name: str = None,
        path: str = None,
        format: str = None,
        sql_schema: str = None,
        alias: TableAlias = None,
        create_statement: str = None,
        is_table: bool = True,
        db_name: str = None,
    ):
        self.name = name or ""
        self.path = path or ""
        self.format = format or ""
        self.sql_schema = sql_schema or ""
        self.alias = alias
        self.create_statement = create_statement
        self.is_table = is_table
        self.db_name = db_name

    @classmethod
    def from_obj(cls, obj):
        if "alias" in obj and len(obj.keys()) > 1:
            raise AssertionError(
                "If alias is used in a configuration, no other key may be used."
            )
        return cls(
            name=obj.get("name"),
            path=obj.get("path"),
            format=obj.get("format"),
            alias=TableAlias(**obj["alias"]) if "alias" in obj else None,
        )

    def __str__(self):
        body = ["TableDetails("]
        if self.name:
            body.append(f"\n  name={self.name}")
        if self.path:
            body.append(f"\n  path={self.path}")
        if self.format:
            body.append(f"\n  format={self.format}")
        if self.sql_schema:
            body.append(f'\n  sql_schema="{self.sql_schema}"')
        if self.alias:
            body.append(f"\n  alias={self.alias}")
        body.append(")")
        return "".join(body)

    def __repr__(self):
        return str(self)
