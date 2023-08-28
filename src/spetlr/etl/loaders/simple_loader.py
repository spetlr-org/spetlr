from typing import List, Protocol, Union

from pyspark.sql import DataFrame

from spetlr.etl import Loader


class Overwritable(Protocol):
    def overwrite(self, df: DataFrame) -> None:
        pass


class Appendable(Protocol):
    def append(self, df: DataFrame) -> None:
        pass


class Upsertable(Protocol):
    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        pass


class SimpleLoader(Loader):
    def __init__(
        self,
        handle: Union[Overwritable, Appendable, Upsertable],
        *,
        mode: str = "overwrite",
        join_cols: List[str] = None,
        dataset_input_keys: Union[str, List[str]] = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)
        self.mode = mode
        self.handle = handle
        self.join_cols = join_cols

    def save(self, df: DataFrame) -> None:
        if self.mode == "overwrite":
            self.handle.overwrite(df)
        elif self.mode == "upsert":
            self.handle.upsert(df, self.join_cols)
        else:
            self.handle.append(df)
