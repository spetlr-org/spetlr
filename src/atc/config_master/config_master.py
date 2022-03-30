from __future__ import annotations

import copy
import importlib.resources
import os
import uuid
from typing import Dict, List

import yaml

from ..singleton import Singleton
from .details import Details, States
from .exceptions import SqlParseException
from .sql import get_details_from_sql


class ConfigMaster(metaclass=Singleton):
    """
    Single point of contact for all configurations regarding tables,
    paths and databases.
    """

    table_details: Dict[str, Details]
    db_details: Dict[str, Details]
    db_name_to_id: Dict[str, str]
    state: States
    ID: str

    def __init__(self):
        self.reset()

    def reset(self) -> ConfigMaster:
        old_self = copy.deepcopy(self)
        self.table_details = {}
        self.db_details = {}
        self.db_name_to_id = {}
        self.state = States.RELEASE
        self.ID = "_" + uuid.uuid4().hex
        return old_self

    def restore(self, other: ConfigMaster) -> ConfigMaster:
        self.table_details = other.table_details
        self.state = other.state
        self.ID = other.ID
        self.db_details = other.db_details
        self.db_name_to_id = other.db_name_to_id
        return self

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
                    self._add_table(id_key, Details.from_obj(obj))
        return self

    def _add_table(self, key: str, details: Details):
        if key in self.table_details:
            raise AssertionError(
                f"The key {key} would overwrite an existing configured table."
            )
        self.table_details[key] = details

    def _add_db(self, key: str, details: Details):
        if key in self.db_details:
            raise AssertionError(
                f"The key {key} would overwrite an existing configured table."
            )
        self.db_details[key] = details
        self.db_name_to_id[details.name] = key

    def sql_from(self, resource_package: str) -> ConfigMaster:
        for path in self._get_resources_of_ext(resource_package, [".sql"]):
            with open(path, encoding="utf-8") as f:
                key = os.path.splitext(os.path.basename(path))[0]
                try:
                    details = get_details_from_sql(f.read())
                except SqlParseException:
                    continue

                if details.is_table:
                    self._add_table(key, details)
                else:
                    self._add_db(key, details)
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

    def _get_dealiassed_object(self, key: str) -> Details:
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

    def get_full_creation_sql(self, key: str) -> str:
        d = self._construct_replacement_dict()
        if key in self.db_details:
            # it's a db. We shoud get all tables that use this db
            obj = self.db_details[key]
            statements = [obj.create_statement.format(**d)]
            for key, details in self.table_details.items():
                if details.db_name != obj.name:
                    continue
                statements.append(details.create_statement.format(**d))
            return statements

        obj = self._get_dealiassed_object(key)
        # we need to find the DB creation statement.
        # without it we cannot be sure the table can be created.
        db = self.db_details[self.db_name_to_id[obj.db_name]]
        return [db.create_statement.format(**d), obj.create_statement.format(**d)]

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
