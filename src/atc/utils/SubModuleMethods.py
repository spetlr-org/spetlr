import importlib
import pkgutil
from types import ModuleType
from typing import Dict, Union

from ..atc_exceptions import DuplicateSchemaNameException


def import_submodules(
    package: Union[str, ModuleType], recursive: bool = True
) -> Dict[str, ModuleType]:
    """Import all submodules of a module, recursively, including subpackages"""
    if isinstance(package, str):
        package = importlib.import_module(package)

    results = {}
    if not hasattr(package, "__path__"):
        results[package.__name__] = package
    else:
        for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
            full_name = package.__name__ + "." + name
            results[full_name] = importlib.import_module(full_name)
            if recursive and is_pkg:
                results.update(import_submodules(full_name))

    return results


def import_objects_of_type(
    package: Union[str, ModuleType],
    object_type: type,
    recursive: bool = True,
    unique_names: bool = True,
) -> Dict[str, object]:

    modules = import_submodules(package=package, recursive=recursive)

    results = {}
    for _, module in modules.items():
        for attr_name, attr in module.__dict__.items():
            if isinstance(attr, object_type):
                if unique_names and attr_name in results:
                    raise DuplicateSchemaNameException(
                        "Unable to auto-configure api client."
                    )
                results[attr_name] = attr

    return results
