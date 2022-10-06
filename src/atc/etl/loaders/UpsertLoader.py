from typing import List

from pyspark.sql import DataFrame

from atc.etl import Loader, dataset_group
from atc.tables import TableHandle


class UpsertLoader(Loader):
    def __init__(self, handle: TableHandle, join_cols: List[str]):
        super().__init__()

        self.handle = handle
        self.join_cols = join_cols

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        """Upserts a single dataframe to the target table."""
        self.handle.upsert(df=df, join_cols=self.join_cols)
