import importlib.resources
import threading
import uuid
from pathlib import Path
from string import Formatter
from types import ModuleType
from typing import Any, Dict, Set, Union

import yaml
from deprecated import deprecated

from spetlr.configurator._cli.ConfiguratorCli import ConfiguratorCli
from spetlr.configurator.sql import _parse_sql_to_config
from spetlr.exceptions import NoSuchValueException
from spetlr.exceptions.configurator_exceptions import DeprecationException
from spetlr.functions import json_hash

# recursive type definition of the details object
TcDetails = Dict[str, Union[str, "TcDetails"]]
TcValue = Union[str, TcDetails]


class ConfiguratorSingleton(type):
    """The reason that we do not use spetlr.singleton here,
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


class Configurator(ConfiguratorCli, metaclass=ConfiguratorSingleton):
    _DEFAULT = object()
    _unique_id: str
    _raw_resource_details: TcDetails
    _is_debug: bool

    _lock: threading.Lock

    # this dict contains all details for all resources
    table_details: Dict[str, str]

    def __init__(
        self,
        resource_path: Union[str, ModuleType] = None,
    ):
        self._lock = threading.Lock()
        self._unique_id = uuid.uuid4().hex
        self.deprecation_errors = False

        self.clear_all_configurations()

        if resource_path:
            self.add_resource_path(resource_path)

    # =============================================================
    # == Internal methods - use no lock, not thead-safe on their own.

    def _all_keys(self):
        return list(self._raw_resource_details.keys())

    def _clear_all_configurations(self):
        self._raw_resource_details = dict()
        self._is_debug = False
        self.table_details = dict()
        self._set_extras()

    def _verify_consistency(self):
        self.table_details = dict()
        return self._get_all_details()

    def _set_extras(self):
        self._register("ID", {"release": "", "debug": f"__{self._unique_id}"})
        self._register("MNT", {"release": "mnt", "debug": "tmp"})

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

                value = self._get_item_property(
                    id_part,
                    property_part,
                    _forbidden_keys.copy(),
                )
                # raises ValueError if it does not exist

                replacements[key] = value
                continue
            except ValueError:
                pass

            # if we get here, there was no _ in the key. Either the key exists as a bare
            # string value, try that:
            try:
                replacements[key] = self._get_item_property(
                    key, "", _forbidden_keys.copy()
                )
                continue
            except NoSuchValueException:
                pass

            # otherwise bare key references are to 'name',
            # which _must_ exist in this case
            replacements[key] = self._get_item_property(
                key, "name", _forbidden_keys.copy()
            )

        # we have run through the key names of all replacement keys in the string.
        # Any that we could not find were skipped silently above, but that means that
        # we do not have them in 'replacements'. Therefore, this next step will raise an
        # exception for any missing key, which will give a meaningful error to the user.
        return raw_string.format(**replacements)

    def _add_resource_path(
        self, resource_path: Union[str, ModuleType], consistency_check=True
    ) -> None:
        self.table_details = dict()

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
                            self._register(key, value)

            # try re-building all details
            if consistency_check:
                self._verify_consistency()
        except:  # noqa: E722  we re-raise the exception, so bare except is ok.
            # this piece makes it so that the Configurator can still be used
            # if any exception raised by the above code is caught.
            self._raw_resource_details = backup_details
            raise

    def _add_sql_resource_path(
        self, resource_path: Union[str, ModuleType], consistency_check=True
    ) -> None:
        self.table_details = dict()

        for key, value in _parse_sql_to_config(resource_path).items():
            self._register(key, value)

        if consistency_check:
            self._verify_consistency()

    def _key_of(self, attribute: str, value: str) -> str:

        for key, object in self._raw_resource_details.items():
            try:
                if object[attribute] == value:
                    return key
            except (KeyError, TypeError):
                continue
        else:
            raise KeyError(f"No key with attribute {attribute}={repr(value)}")

    def __reset(self, debug: bool, deprecation_errors=False) -> None:
        self._is_debug = debug
        self.deprecation_errors = deprecation_errors
        self.table_details = dict()

    def _register(self, key: str, value: TcValue) -> str:

        if value is None:
            self._raw_resource_details.pop(key, None)
        elif isinstance(value, dict) and isinstance(
            self._raw_resource_details.get(key), dict
        ):
            # if both are dicts, update the contents.
            self._raw_resource_details[key].update(value)

            # This is how to clear a key if the keyvalue is None
            self._raw_resource_details[key] = {
                k: v
                for k, v in self._raw_resource_details[key].items()
                if v is not None
            }

            # If the key has no remaining values, remove it completely
            if not self._raw_resource_details[key]:
                self._raw_resource_details.pop(key)

        else:
            self._raw_resource_details[key] = value

        self.table_details = dict()
        return key

    def _define(self, **kwargs) -> str:
        if not kwargs:
            raise ValueError("No value passed.")

        key = json_hash(kwargs)
        return self._register(key, kwargs)

    def _regenerate_unique_id_and_clear_conf(self):
        self._unique_id = uuid.uuid4().hex
        self._clear_all_configurations()

    def _get_all_details(self):

        if not self.table_details:
            self.table_details = dict()

            for table_id in self._raw_resource_details.keys():
                # add the name as the bare key
                try:
                    self.table_details[table_id] = self._get(table_id)
                    continue  # if it was a bare string, we can stop here
                except NoSuchValueException:
                    pass

                try:
                    self.table_details[table_id] = self._get(table_id, "name")
                except NoSuchValueException:
                    pass

                # add every property as a _property part
                for property_name in set(self._get_item(table_id).keys()):
                    try:
                        item = self._get(table_id, property_name)
                    except NoSuchValueException:
                        continue
                    # if the dict values are dicts, stop here,
                    # not supported for direct substitution
                    # this will take care of definitions of schema and similar.
                    if not isinstance(item, dict):
                        self.table_details[f"{table_id}_{property_name}"] = str(item)

        return self.table_details

    def _get(self, table_id: str, property: str = "", default: Any = _DEFAULT):
        try:
            return self._get_item_property(table_id, property)
        except NoSuchValueException:
            if default is self._DEFAULT:
                raise
            else:
                return default

    # ================================================================
    # == Interface methods - use lock, can be used in threaded context

    def clear_all_configurations(self):
        with self._lock:
            return self._clear_all_configurations()

    def all_keys(self):
        """All keys that appear in the configuration files."""
        with self._lock:
            return self._all_keys()

    def verify_consistency(self):
        """This method will re-build the table details. This requires that all
        internally recursive references can be resolved."""

        with self._lock:
            return self._verify_consistency()

    @deprecated(
        reason="use .get('ENV') to get literal values.",
    )
    def get_extra_details(self) -> Dict[str, str]:
        """The distinction between extras and normal values was removed.
        This method will always return an empty dict. Extra settings are now
        string items. Retrieve them with .get(id)"""
        self._deprecated()

        return {}

    def add_resource_path(
        self, resource_path: Union[str, ModuleType], consistency_check=True
    ) -> None:
        with self._lock:
            return self._add_resource_path(resource_path, consistency_check)

    def add_sql_resource_path(
        self, resource_path: Union[str, ModuleType], consistency_check=True
    ) -> None:
        with self._lock:
            return self._add_sql_resource_path(resource_path, consistency_check)

    def key_of(self, attribute: str, value: str) -> str:
        """Obtain the key of the first registered item that has a given attribute
        set to a given value. Uniqueness of the match is the responsibility of
        the library user.

        This function is slow as it performs a linear search. It is intended for use in
        setup code.
        """
        with self._lock:
            return self._key_of(attribute, value)

    def _deprecated(self):
        if self.deprecation_errors:
            raise DeprecationException(
                "A deprecated feature was used and deprecation_errors was set to True."
            )

    @deprecated(
        reason="register literal string values instead.",
    )
    def set_extra(self, **kwargs: str):
        """Use .register(key,value) instead.
        for example call .register('ENV','prod')"""
        self._deprecated()

        with self._lock:
            for key, value in kwargs.items():
                self._register(key, value)

    def set_debug(self, deprecation_errors=False):
        """Select debug tables. {ID} will be replaced with a guid"""
        with self._lock:
            return self.__reset(debug=True, deprecation_errors=deprecation_errors)

    def set_prod(self):
        """Select production tables. {ID} will be replaced with a empty string"""
        with self._lock:
            return self.__reset(debug=False, deprecation_errors=False)

    def reset(self, *, debug: bool = False, deprecation_errors=False):
        """
        Resets table names and table SQL. Enables or disables debug mode
        (used for unit tests and integration tests).
        :param debug: False -> release tables, True -> debug tables.
        :param kwargs: additional keys to be substituted in names and paths
        """
        with self._lock:
            return self.__reset(debug, deprecation_errors=deprecation_errors)

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

    def register(self, key: str, value: TcValue) -> str:
        """
        Register a new item and return its key.
        If both the new and old items are dictionaries, their contents are merged.
        Supply value=None to clear a key.
        """
        with self._lock:
            return self._register(key, value)

    def define(self, **kwargs) -> str:
        """
        Register a new item based on its properties only and return its key.
        The returned key is a hash-like string depending only on the values.
        """
        with self._lock:
            return self._define(**kwargs)

    def table_name(self, table_id: str):
        """
        Return the table name for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :return: str: table name
        """
        with self._lock:
            return self._get(table_id, "name")

    @deprecated(
        reason='Use .get(table_id,"path") instead.',
    )
    def table_path(self, table_id: str):
        """
        Return the table path for the specified table id.
        :param table_id: Table id in the .json or .yaml files.
        :return: str: table path
        """
        self._deprecated()
        with self._lock:
            return self._get(table_id, "path")

    def get(self, table_id: str, property: str = "", default: Any = _DEFAULT):
        """return the property of the table_id.
        To get raw strings, specify no property.
        If default is set, it is returned instead of raising in case of missing keys.
        Return value will be whatever was registered under the given property."""

        with self._lock:
            return self._get(table_id, property, default=default)

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
        with self._lock:
            return self._get_all_details()

    def regenerate_unique_id_and_clear_conf(self):
        with self._lock:
            return self._regenerate_unique_id_and_clear_conf()

    @deprecated(
        reason="Use .get(table_id,property_name) instead.",
    )
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
        self._deprecated()

        property_value = self.get_all_details().get(
            f"{table_id}_{property_name}", default_value
        )

        if property_value is None:
            raise ValueError(
                f"property '{property_name}' for table identifier '{table_id}' is empty"
            )
        return property_value
