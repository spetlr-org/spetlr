from __future__ import annotations

from enum import Enum
import importlib.resources
import os
from typing import List, Dict
import copy
from collections import namedtuple
import yaml
import sqlparse
import uuid
from .singleton import Singleton


class States(Enum):
    RELEASE = 0
    DEBUG = 1


TableAlias = namedtuple("TableAlias", [e.name.lower() for e in States])


class TableDetails:
    def __init__(
        self,
        name: str = None,
        path: str = None,
        format: str = None,
        sql_schema: str = None,
        alias: TableAlias = None,
        create_statement: str = None,
    ):
        self.name = name or ""
        self.path = path or ""
        self.format = format or ""
        self.sql_schema = sql_schema or ""
        self.alias = alias
        self.create_statement = create_statement

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


class NoTableCreationException(Exception):
    pass


class ConfigMaster(metaclass=Singleton):
    """
    Single point of contact for all configurations regarding tables, paths and databases.
    """

    table_details: Dict[str, TableDetails]
    state: States
    ID: str

    def __init__(self):
        self.reset()

    def reset(self, other: ConfigMaster = None) -> ConfigMaster:
        old_self = copy.deepcopy(self)
        if other:
            self.table_details = other.table_details
            self.state = other.state
            self.ID = other.ID
        else:
            self.table_details = {}
            self.state = States.RELEASE
            self.ID = "_" + uuid.uuid4().hex
        return old_self

    def set_debug(self) -> ConfigMaster:
        self.state = States.DEBUG
        return self

    def set_release(self) -> ConfigMaster:
        self.state = States.RELEASE
        return self

    def extras_from(self, resource_package: str) -> ConfigMaster:
        for path in self._get_resources_of_ext(resource_package, [".yml", ".yaml"]):
            with open(path, encoding="utf-8") as f:
                for id_key, obj in yaml.load(f, Loader=yaml.FullLoader).items():
                    self._add_item(id_key, TableDetails.from_obj(obj))
        return self

    def _add_item(self, key: str, details: TableDetails):
        if key in self.table_details:
            raise AssertionError(
                f"The key {key} would overwrite an existing configured table."
            )
        self.table_details[key] = details

    def sql_from(self, resource_package: str) -> ConfigMaster:
        for path in self._get_resources_of_ext(resource_package, [".sql"]):
            with open(path, encoding="utf-8") as f:
                key = os.path.splitext(os.path.basename(path))[0]
                try:
                    details = self._get_details_from_sql(f.read())
                except NoTableCreationException:
                    continue
                self._add_item(key, details)

        return self

    def _construct_replacement_dict(self):
        d = {}
        d.update(self.table_details)
        if self.state == States.RELEASE:
            d["ID"] = ""
        else:
            d["ID"] = self.ID
        return d

    def get_name(self, key: str) -> str:
        d = self._construct_replacement_dict()
        obj = self._get_dealiassed_object(key)
        return obj.name.format(**d)

    def _get_dealiassed_object(self, key: str):
        obj = self.table_details[key]
        while obj.alias:
            if self.state == States.RELEASE:
                obj = self.table_details[obj.alias.release]
            else:
                obj = self.table_details[obj.alias.debug]
        return obj

    def get_path(self, key: str) -> str:
        d = self._construct_replacement_dict()
        obj = self._get_dealiassed_object(key)
        return obj.path.format(**d)

    def get_sql(self, key: str) -> str:
        d = self._construct_replacement_dict()
        obj = self._get_dealiassed_object(key)
        return obj.create_statement.format(**d)

    def _get_details_from_sql(self, sql: str) -> TableDetails:
        sql = self._remove_sql_comment_lines(sql)

        tree = sqlparse.parse(sql.strip())[0]
        clean_tree = [
            token for token in tree if not token.is_whitespace
        ]  # we don't want whitespaces

        # does this define a table?
        token0, token1, token2 = (
            clean_tree[0].value.upper(),
            clean_tree[1].value.upper(),
            clean_tree[2].value.upper(),
        )
        if token1 == "EXTERNAL":
            token1 = token2
        if f"{token0} {token1}" != "CREATE TABLE":
            raise NoTableCreationException()

        # the first identifier is the name
        table_name_token = [
            token for token in clean_tree if isinstance(token, sqlparse.sql.Identifier)
        ][0]
        table_name = table_name_token.value.strip('"')

        # by inspection of the CREATE TABLE syntax found here:
        # https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table.html
        # the schema will always be a parenthesis that starts right after the name
        sql_schema_token_candidate = clean_tree[clean_tree.index(table_name_token) + 1]
        if isinstance(sql_schema_token_candidate, sqlparse.sql.Parenthesis):
            table_sql_schema = (
                str(sql_schema_token_candidate).strip().strip("()").strip()
            )
        else:
            table_sql_schema = None

        # get the path
        path_token = [
            token for token in clean_tree if token.value.upper() == "LOCATION"
        ][0]
        table_path = clean_tree[clean_tree.index(path_token) + 1].value.strip('"')

        return TableDetails(
            name=table_name,
            path=table_path,
            sql_schema=table_sql_schema,
            create_statement=sql,
        )

    @staticmethod
    def _remove_sql_comment_lines(sql: str) -> str:
        lines = []
        for line in sql.splitlines(keepends=False):
            line = line.strip()
            if line.startswith("--"):
                continue
            lines.append(line)
        return (" ".join(lines)).strip()

    def _get_resources_of_ext(self, resource_package: str, extensions: List[str]):
        for path in self._get_nested_resources(resource_package):
            _, ext = os.path.splitext(path)
            if ext in extensions:
                yield path

    def _get_nested_resources(self, resource_package: str):
        for resource in importlib.resources.contents(resource_package):
            if str(resource).startswith("__"):
                continue
            with importlib.resources.path(resource_package, resource) as resource_path:
                if os.path.isdir(resource_path):
                    for root, dirs, files in os.walk(resource_path):
                        for file in files:
                            if str(file).startswith("__"):
                                continue
                            yield os.path.join(root, file)
                else:
                    yield resource_path
