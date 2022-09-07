from abc import ABC
from typing import List

from pyspark.sql import DataFrame


class TableHandle(ABC):
    def read(self) -> DataFrame:
        pass

    def overwrite(self, df: DataFrame) -> None:
        pass

    def append(self, df: DataFrame) -> None:
        pass

    def upsert(self, df: DataFrame, join_cols: List[str]) -> None:
        pass

    def truncate(self) -> None:
        pass

    def drop(self) -> None:
        pass

    def drop_and_delete(self) -> None:
        pass
