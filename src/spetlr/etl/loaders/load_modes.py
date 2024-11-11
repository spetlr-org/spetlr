from typing import List, Protocol, Union

from pyspark.sql import DataFrame


class Upsertable(Protocol):
    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        pass


class Overwritable(Protocol):
    def overwrite(
        self,
        df: DataFrame,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
        *,
        overwritePartitions: bool = None,
    ) -> None:
        pass


class Appendable(Protocol):
    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        pass
