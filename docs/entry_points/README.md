# Entry points

The `TaskEntryPoint` is an abstract base class that acts as an entry point for tasks. This class is meant to be subclassed and extended with specific implementation details. The subclass must implement a method `task()` which serves as the entry point for the task logic.

## Usage

To use `TaskEntryPoint`, create a subclass that implements the `task()` method:
```python
class MyTask(TaskEntryPoint):
            @classmethod
            def task(cls) -> None:
                # implement the task logic here
```

It is now ensured that there is a `task()` method of the subclass `MyTask`, and we therefore have an entry point at `MyTask.task()`. This entry point can be added to the python wheel either manually or via automatic discovery.