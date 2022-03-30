import importlib.resources
import json
import uuid
from pathlib import Path
from types import ModuleType
from typing import Dict, Set, TypedDict, Union

import yaml

from atc.config_master.exceptions import UnknownShapeException
from atc.singleton import Singleton


class TableAlias(TypedDict):
    alias: str


class TableDetails(TypedDict):
    name: str
    path: str
    format: str
    date: str


class TableRelDbg(TypedDict):
    release: Union[TableDetails, TableAlias]
    debug: Union[TableDetails, TableAlias]


TableDetails_keys = set(TableDetails.__annotations__.keys())
TableAlias_keys = set(TableAlias.__annotations__.keys())
TableRelDbg_keys = set(TableRelDbg.__annotations__.keys())


class TableConfigurator(metaclass=Singleton):
    unique_id: str
    table_names: Dict[str, Union[TableDetails, TableAlias, TableRelDbg]]
    table_arguments: Dict[str, str]
    resource_paths: Set[Union[str, ModuleType]]

    def __init__(self, resource_path: Union[str, ModuleType] = None):
        self.unique_id = uuid.uuid4().hex
        self.resource_paths = set()
        self.table_names = dict()

        if resource_path:
            self.add_resource_path(resource_path)

        self.__reset()

    def add_resource_path(self, resource_path: Union[str, ModuleType]) -> None:
        self.resource_paths.add(resource_path)

    def __reset(self, debug: bool = False, **kwargs) -> None:
        self._is_debug = debug
        self.table_arguments = dict()
        self.table_arguments["MNT"] = "tmp" if debug else "mnt"
        self.table_arguments["ID"] = f"__{self.unique_id}" if debug else ""
        self.table_arguments.update(kwargs)

        self.table_names = dict()
        for resource_path in self.resource_paths:
            for file_name in importlib.resources.contents(resource_path):
                extension = Path(file_name).suffix
                if extension in [".json", ".yaml", ".yml"]:
                    with importlib.resources.path(
                        resource_path, file_name
                    ) as file_path:
                        with open(file_path) as file:
                            update = (
                                json.load(file)
                                if extension == ".json"
                                else yaml.load(file, Loader=yaml.FullLoader)
                            )
                            if not isinstance(update, dict):
                                raise ValueError(f"document in {file_path} is no dict.")

                            for key, value in update.items():
                                if self.__is_known_shape(value):
                                    self.table_names[key] = value
                                else:
                                    raise UnknownShapeException(
                                        f"Object {key} in file {file_path}"
                                        f" has unexpected shape."
                                    )

        for key in self.table_names.keys():
            self.__resolve_key(key)

    def __is_TableDetails_shape(self, value):
        return set(value.keys()).issubset(TableDetails_keys)

    def __is_TableAlias_shape(self, value):
        return set(value.keys()).issubset(TableAlias_keys)

    def __is_TableRelDbg_shape(self, value):
        return set(value.keys()).issubset(TableRelDbg_keys)

    def __is_known_shape(self, value):
        return (
            self.__is_TableDetails_shape(value)
            or self.__is_TableAlias_shape(value)
            or self.__is_TableRelDbg_shape(value)
        )

    def __resolve_key(self, key: str):
        # Handle the case of differentiated release and debug tables
        if self.__is_TableRelDbg_shape(self.table_names[key]):
            value: TableRelDbg = self.table_names[key]
            if self._is_debug:
                self.table_names[key] = value["debug"]
            else:
                self.table_names[key] = value["release"]

        # carry out string substitutions in name and path
        if self.__is_TableDetails_shape(self.table_names[key]):
            value: TableDetails = self.table_names[key]
            if "name" in value:
                self.table_names[key]["name"] = value["name"].format(
                    **self.table_arguments
                )
            if "path" in value:
                self.table_names[key]["path"] = value["path"].format(
                    **self.table_arguments
                )

    def __get_name_entry(self, table_id: str):
        entry = self.table_names[table_id]

        # this stack allows us to detect alias loop
        stack = {table_id}

        while "alias" in entry:  # allow alias of alias
            new_id = entry["alias"]
            if new_id in stack:
                raise ValueError(f"Alias loop at key {new_id}")
            stack.add(new_id)
            entry = self.table_names[new_id]
        return entry

    def reset(self, *, debug: bool = False, **kwargs):
        """
        Resets table names and table SQL. Enables or disables debug mode
        (used for unit tests and integration tests).
        :param debug: False -> release tables, True -> debug tables.
        :param kwargs: additional keys to be substituted in names and paths
        """
        self.__reset(debug, **kwargs)

    def is_debug(self):
        """
        Return True if table names and table SQL specify debug table,
        False if release tables
        """
        return self._is_debug

    def get_unique_id_length(self):
        """
        Return the character length of the UUID identifier inserted into
        names with the {ID} tag
        """
        return len(self.unique_id)

    def register(self, key: str, value: Union[TableDetails, TableAlias, TableRelDbg]):
        """
        Register a new table.
        """
        if not self.__is_known_shape(value):
            raise ValueError("Object has unexpected shape.")
        self.table_names[key] = value
        self.__resolve_key(key)

    def table_property(
        self, table_id: str, property_name: str, default_value: str = None
    ):
        """
        Return the table property (e.g. name, path, format, etc.)
            for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :param property_name: Name of the property to read (e.g. "name", "path", etc.)
        :param default_value: Optional default value of the property
            if the property is missing.
        :return: str: property value
        """
        property_value = self.__get_name_entry(table_id).get(
            property_name, default_value
        )
        if property_value is None:
            raise ValueError(
                f"property '{property_name}' for table identifier '{table_id}' is empty"
            )
        return property_value

    def table_name(self, table_id: str):
        """
        Return the table name for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :return: str: table name
        """
        return self.table_property(table_id, "name")

    def table_path(self, table_id: str):
        """
        Return the table path for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :return: str: table path
        """
        return self.table_property(table_id, "path")

    def get_all_details(self):
        table_objects = {}
        for key in self.table_names.keys():
            table = self.__get_name_entry(key)

            for property, attribute in table.items():
                table_objects[f"{key}_{property}"] = attribute

            if f"{key}_name" in table_objects:
                table_objects[key] = table_objects[f"{key}_name"]

        return table_objects
