from typing import TypedDict, Union

# These typed dicts are listed separately to allow type-checking


class TableAlias(TypedDict):
    alias: str


class TableDetailsName(TypedDict):
    name: str


class TableDetails(TableDetailsName):
    path: str


class TableDetailsExtended(TableDetails):
    format: str
    partitioning: str


class TableRelDbg(TypedDict):
    release: Union[TableDetailsName, TableDetails, TableDetailsExtended, TableAlias]
    debug: Union[TableDetailsName, TableDetails, TableDetailsExtended, TableAlias]


TableDetails_keys = set(TableDetails.__annotations__.keys())
TableDetailsExtended_keys = set(TableDetailsExtended.__annotations__.keys())
TableAlias_keys = set(TableAlias.__annotations__.keys())
TableRelDbg_keys = set(TableRelDbg.__annotations__.keys())

AnyDetails = Union[
    TableDetailsName, TableDetails, TableDetailsExtended, TableAlias, TableRelDbg
]
