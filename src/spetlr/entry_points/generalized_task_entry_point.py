import importlib
import sys

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
    for arg in sys.argv:
        if not arg.startswith("--"):
            continue
        k, v = arg[2:].split("=")
        if k == ENTRY_POINT:
            entry_point = v
        else:
            kwargs[k] = v
    if entry_point is None:
        raise Exception("No entry_point specified.")

    mod_name, func_name = entry_point.split(":")
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)

    return func(**kwargs)
