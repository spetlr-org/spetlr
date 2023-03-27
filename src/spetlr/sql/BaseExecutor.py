from typing import Protocol


class BaseExecutor(Protocol):
    def sql(self, statement: str) -> None:
        pass
