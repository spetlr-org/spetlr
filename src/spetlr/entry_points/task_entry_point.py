from abc import ABC, abstractmethod


class TaskEntryPoint(ABC):
    """
    An abstract base class that acts as an entry point for tasks.

    This class is meant to be subclassed and extended with specific implementation
    details. The subclass must implement
    a method `task()` which serves as the entry point for the task logic.

    Usage:
        To use `TaskEntryPoint`, create a subclass that implements the `task()` method,
        and call it as follows:

        class MyTask(TaskEntryPoint):
            @classmethod
            def task(cls) -> None:
                # implement the task logic here

        MyTask.task()

    Raises:
        NotImplementedError: If `task()` method is not implemented in the subclass.
    """

    @classmethod
    @abstractmethod
    def task(cls) -> None:
        """
        Abstract class method that serves as the entry point for the task logic.
        This method should be implemented by the subclass with the specific
        implementation details of the task.
        """
        raise NotImplementedError()
