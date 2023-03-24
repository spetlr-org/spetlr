import importlib.resources
import uuid
from pathlib import Path
from string import Formatter
from types import ModuleType
from typing import Dict, Set, Union

import yaml
from deprecated import deprecated

from atc.configurator.sql import _parse_sql_to_config
from atc.exceptions import NoSuchValueException

# recursive type definition of the details object
TcDetails = Dict[str, Union[str, "TcDetails"]]
TcValue = Union[str, TcDetails]


class ConfiguratorSingleton(type):
    """The reason that we do not use atc.singleton here,
    is that the behavior of that metaclass depends on the classname.
    Which prevents us from making a deprecated subclass of the old name
    which actually instantiates to the new class instance."""

    _instance = None

    def __call__(cls, *args, **kwargs):
        if ConfiguratorSingleton._instance is None:
            ConfiguratorSingleton._instance = super(
                ConfiguratorSingleton, cls
            ).__call__(*args, **kwargs)
        return ConfiguratorSingleton._instance


class Configurator(metaclass=ConfiguratorSingleton):
    _unique_id: str
    _raw_resource_details: TcDetails
    _is_debug: bool

    # this dict contains all details for all resources
    table_details: Dict[str, str]

    def __init__(
        self,
        resource_path: Union[str, ModuleType] = None,
    ):
        self._unique_id = uuid.uuid4().hex
        self.clear_all_configurations()

        if resource_path:
            self.add_resource_path(resource_path)

    def all_keys(self):
        """All keys that appear in the configuration files."""
        return list(self._raw_resource_details.keys())

    def clear_all_configurations(self):
        self._raw_resource_details = dict()
        self._is_debug = False
        self.table_details = dict()
        self._set_extras()

    ############################################
    # the core logic of this class is contained
    # in the following methods
    ############################################

    def _set_extras(self):
        self.register("ID", {"release": "", "debug": f"__{self._unique_id}"})
        self.register("MNT", {"release": "mnt", "debug": "tmp"})

    @deprecated(
        reason="use .get('ENV') to get literal values.",
    )
    def get_extra_details(self) -> Dict[str, str]:
        """The distinction between extras and normal values was removed.
        This method will always return an empty dict. Extra settings are now
        string items. Retrieve them with .get(id)"""
        return {}

    def _get_item(self, table_id: str) -> TcValue:
        """item dictionary where release-debug and alias loops are resolved"""
        # explanation to the maintainer:
        # There are many ways to specify a value in the Configurator:
        # MyLiteral: "MyValue"
        #
        # MyKey:
        #   name: someName
        #   path: some/Path
        #
        # MyAlias:
        #   alias: MyKey
        #
        # MyForked:
        #   release:
        #     alias: MyAlias
        #   debug:
        #     name: anotherName
        #     path: another/Path
        #
        # What we want is to support that in all cases we get the intended value out
        # tc._get_item("MyLiteral") -> "MyValue"
        # tc._get_item("MyKey") -> {'name':..., 'path':...}
        # tc._get_item("MyAlias") -> same as tc._get_item("MyKey")
        # tc._get_item("MyForked")
        #   if not self._is_debug -> same as tc._get_item("MyKey")
        #   if self._is_debug -> {'name':'anotherName', 'path':'another/Path'}
        #
        # resolving forks and aliases is the task of this method.

        # this stack allows us to detect alias loops
        stack = {table_id}

        value: TcValue = self._raw_resource_details[table_id]

        while True:
            if not isinstance(value, dict):
                # situation like MyLiteral
                return value
            else:
                # value is a dict
                if set(value.keys()) == {"release", "debug"}:
                    # Situation like MyForked
                    if self._is_debug:
                        value = value["debug"]
                    else:
                        value = value["release"]
                    continue
                elif set(value.keys()) == {"alias"}:  # allow alias of alias
                    # Situation like MyAlias
                    new_id = value["alias"]
                    if new_id in stack:
                        raise ValueError(f"Alias loop at key {new_id}")
                    stack.add(new_id)
                    value = self._raw_resource_details[new_id]
                    continue
                else:
                    # value is a dict,
                    # but it is neither a forking (release,debug)
                    # nor is it an alias
                    # hence we have arrived at the final value set of this item
                    # This is how both MyForked and MyAlias can arrive at MyKey
                    return value

    def _get_unsubstituted_item_property(self, table_id: str, property: str) -> str:
        """item property where release-debug and alias loops are resolved"""
        value = self._get_item(table_id)
        if not property:
            if isinstance(value, dict):
                raise NoSuchValueException("Cannot get bare string. Item is a Dict")
            else:
                return value
        if property in value:
            return value[property]
        else:
            raise NoSuchValueException(property)

    def _get_item_property(
        self, table_id: str, property: str, _forbidden_keys: Set[str] = None
    ) -> str:
        """Get the full item property, fully resolved and substituted."""
        raw_string = self._get_unsubstituted_item_property(table_id, property)

        # some items are not strings, then the rest of this function makes no sense
        if not isinstance(raw_string, str):
            return raw_string

        # get all keys used in the raw_string, such as using {MyDb} will get "MyDb"
        format_keys = set(
            i[1] for i in Formatter().parse(raw_string) if i[1] is not None
        )

        # the forbidden-keys logic allows us to detect reference loops.
        # no key that is in the upstream of a property is allowed in the string
        # substitutions of this property.
        _forbidden_keys = _forbidden_keys or set()
        composite_key = table_id
        if property:
            composite_key += f"_{property}"
        _forbidden_keys.add(composite_key)
        if property == "name":
            _forbidden_keys.add(table_id)
        if any(key in _forbidden_keys for key in format_keys):
            raise ValueError(
                f"Substitution loop at table {table_id} property {property}"
            )

        # every key that is not in the special properties and not forbidden
        # should refer to another resource. If it does not an exception will occur.
        replacements = {}
        for key in format_keys:
            try:
                # maybe we have a case of reference to property, let's try:

                id_part, property_part = key.rsplit("_", 1)
                # will raise ValueError if there are too few parts,
                # keys without _ are handled below

                value = self._get_item_property(id_part, property_part, _forbidden_keys)
                # raises ValueError if it does not exist

                replacements[key] = value
                continue
            except ValueError:
                pass

            # if we get here, there was no _ in the key. Either the key exists as a bare
            # string value, try that:
            try:
                replacements[key] = self._get_item_property(key, "", _forbidden_keys)
                continue
            except NoSuchValueException:
                pass

            # otherwise bare key references are to 'name',
            # which _must_ exist in this case
            replacements[key] = self._get_item_property(key, "name", _forbidden_keys)

        # we have run through the key names of all replacement keys in the string.
        # Any that we could not find were skipped silently above, but that means that
        # we do not have them in 'replacements'. Therefore, this next step will raise an
        # exception for any missing key, which will give a meaningful error to the user.
        return raw_string.format(**replacements)

    def add_resource_path(self, resource_path: Union[str, ModuleType]) -> None:
        backup_details = self._raw_resource_details.copy()
        try:
            for file_name in importlib.resources.contents(resource_path):
                extension = Path(file_name).suffix
                if extension not in [".json", ".yaml", ".yml"]:
                    continue
                with importlib.resources.path(resource_path, file_name) as file_path:
                    with open(file_path) as file:
                        # just use yaml since json is a subset of yaml
                        update = yaml.load(file, Loader=yaml.FullLoader)

                        if not isinstance(update, dict):
                            raise ValueError(f"document in {file_path} is no dict.")

                        for key, value in update.items():
                            # we now support all bare value types in yaml.
                            # no further checking
                            self._raw_resource_details[key] = value

            # try re-building all details
            self.table_details = dict()
            self.get_all_details()
        except:  # noqa: E722  we re-raise the exception, so bare except is ok.
            # this piece makes it so that the Configurator can still be used
            # if any exception raised by the above code is caught.
            self._raw_resource_details = backup_details
            raise

    def add_sql_resource_path(self, resource_path: Union[str, ModuleType]) -> None:
        self._raw_resource_details.update(_parse_sql_to_config(resource_path))

    ############################################
    # all methods below are interface and convenience methods
    ############################################

    @deprecated(
        reason="register literal string values instead.",
    )
    def set_extra(self, **kwargs: str):
        """Use .register(key,value) instead.
        for example call .register('ENV','prod')"""
        for key, value in kwargs.items():
            self.register(key, value)

    def set_debug(self):
        """Select debug tables. {ID} will be replaced with a guid"""
        self.reset(debug=True)

    def set_prod(self):
        """Select production tables. {ID} will be replaced with a empty string"""
        self.reset(debug=False)

    def __reset(self, debug: bool) -> None:
        self._is_debug = debug
        self.table_details = dict()

    def reset(self, *, debug: bool = False):
        """
        Resets table names and table SQL. Enables or disables debug mode
        (used for unit tests and integration tests).
        :param debug: False -> release tables, True -> debug tables.
        :param kwargs: additional keys to be substituted in names and paths
        """
        self.__reset(debug)

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
        return len(self._unique_id)

    def register(self, key: str, value: TcValue):
        """
        Register a new item.
        """
        self._raw_resource_details[key] = value
        self.table_details = dict()

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
        property_value = self.get_all_details().get(
            f"{table_id}_{property_name}", default_value
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

    @deprecated(
        reason='Use .get(table_id,"path") instead.',
    )
    def table_path(self, table_id: str):
        """
        Return the table path for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :return: str: table path
        """
        return self.get(table_id, "path")

    def get(self, table_id: str, property: str = "") -> str:
        return self._get_item_property(table_id, property)

    def get_all_details(self):
        """
        Return a dictionary containing every resource detail fully resolved.
        e.g. a resource that looks like this:
        MyTableDetail:
            name: mytablename
            path: my/table/path
        will appear with three keys:
            - "MyTableDetail"
            - "MyTableDetail_name"
            - "MyTableDetail_path"
        all substitutions will be fully resolved.
        """
        if not self.table_details:
            self.table_details = dict()

            for table_id in self._raw_resource_details.keys():
                # add the name as the bare key
                try:
                    self.table_details[table_id] = self.get(table_id)
                    continue  # if it was a bare string, we can stop here
                except NoSuchValueException:
                    pass

                try:
                    self.table_details[table_id] = self.get(table_id, "name")
                except NoSuchValueException:
                    pass

                # add every property as a _property part
                for property_name in set(self._get_item(table_id).keys()):
                    try:
                        item = self.get(table_id, property_name)
                    except NoSuchValueException:
                        continue
                    # if the dict values are dicts, stop here,
                    # not supported for direct substitution
                    # this will take care of definitions of schema and similar.
                    if not isinstance(item, dict):
                        self.table_details[f"{table_id}_{property_name}"] = str(item)

        return self.table_details

    from ._cli import cli
