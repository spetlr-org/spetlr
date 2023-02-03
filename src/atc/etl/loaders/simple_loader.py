from typing import List, Protocol, Union

from pyspark.sql import DataFrame

from atc.etl import Loader


class Overwritable(Protocol):
    def overwrite(self, df: DataFrame) -> None:
        pass


class Appendable(Protocol):
    def append(self, df: DataFrame) -> None:
        pass


class SimpleLoader(Loader):
    def __init__(
        self,
        handle: Union[Overwritable, Appendable],
        *,
        mode: str = "overwrite",
        dataset_input_keys: Union[str, List[str]] = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)
        self.mode = mode
        self.handle = handle

    def save(self, df: DataFrame) -> None:
        if self.mode == "overwrite":
            self.handle.overwrite(df)
        else:
            self.handle.append(df)
