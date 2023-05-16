# Entry points

## Entry Point Discovery

The `TaskEntryPoint` is an abstract base class that acts as an entry point for tasks.
This class is meant to be subclassed and extended with specific implementation 
details.  The subclass must implement a method `task()` which serves as the entry 
point for the task logic.

### Usage

To use `TaskEntryPoint`, create a subclass that implements the `task()` method:
```python
class MyTask(TaskEntryPoint):
            @classmethod
            def task(cls) -> None:
                # implement the task logic here
```

It is now ensured that there is a `task()` method of the subclass `MyTask`, and we
therefore have an entry point at `MyTask.task()`. This entry point can be added to
the python wheel either manually or via automatic discovery.

## Light Entry Point

Normally, to call code within a python wheel, an entry point must be specified in 
the `setup.cfg` file. To skip this part, spetlr provides a standard entry point, 
`spetlr_task`. Using this entry point, any other library code can be called.

### Usage
If you want to call this function:

```python
    def myfunction(myarg='default'): 
        pass
```

that is residing in the file `mylib.myfolder.myfile`,
then specify your task as follows:

```json
{
  "python_wheel_task": {
    "package_name": "spetlr",
    "entry_point": "spetlr_task",
    "named_parameters": {
      "entry_point": "mylib.myfolder.myfile:myfunction",
      "myarg": "myval"
    }
  }
}
```

The `"package_name": "spetlr"` and  `"entry_point": "spetlr_task"` should always be 
kept. The mandatory parameter 'entry_point' then points to the code to be called. 
All further arguments, all of type `string`, will be passed to the called function as 
named parameters.
