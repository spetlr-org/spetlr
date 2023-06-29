import importlib
import inspect
import sys
from typing import Any, Callable, Dict

ENTRY_POINT = "entry_point"


def main():
    """This function is entry point from which another entry point can be called.
    If you want to call this function:
    ```python
        def myfunction(myarg='default'): pass
    ```
    that is residing in the folder `mylib.myfolder.myfile`,
    then specify your task as follows:
    ```json
        "python_wheel_task": {
            "package_name": "spetlr",
            "entry_point": "spetlr_task",
            "named_parameters": {
                "entry_point": "mylib.myfolder.myfile:myfunction",
                "myarg": "myval"
            }
        }
    ```
    The named parameter 'entry_point' is mandatory.
    All arguments must be of type string.
    """

    kwargs = {}
    entry_point = None
    # sys.argv will contain strings like
    # [ "--entry_point=my.module:main", "--myarg=myval" ]
    for arg in sys.argv:
        # parameters of any other form are ignored
        if not arg.startswith("--"):
            continue
        if arg.find("=") < 0:
            continue

        k, v = arg[2:].split("=", 1)

        if k == ENTRY_POINT:
            # magic mandatory parameter
            entry_point = v
        else:
            # any other custom parameters
            kwargs[k] = v

    if entry_point is None:
        raise Exception("No entry_point specified.")

    # entry_point looks like "my.module:main"
    modname, qualname_separator, qualname = entry_point.partition(":")

    obj = importlib.import_module(modname)
    if qualname_separator:
        for attr in qualname.split("."):
            obj = getattr(obj, attr)

    kwargs = prepare_keyword_arguments(obj, kwargs)

    # call the callable with custom parameters
    return obj(**kwargs)


def prepare_keyword_arguments(callable_obj: Callable, kwargs_dict: Dict[str, Any]):
    """Reduce the dict down to a set of keys that the callable can actually be called
    with. Any extra keys are dropped with a warning."""
    signature = inspect.signature(callable_obj)
    parameters = signature.parameters

    if any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ):
        # if any of the callable's parameters use the form **kwargs,
        # then we don't need to check further, just pass everything,
        return kwargs_dict

    valid_kwargs = {}
    for key, value in kwargs_dict.items():
        if key in parameters:  # only proceed if the parameter exists on the callable
            valid_kwargs[key] = value
        else:
            print(
                f"WARNING: Ignoring job parameter: {key}. "
                "The entry point cannot receive it."
            )

    return valid_kwargs
