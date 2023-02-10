from typing import List, Union

from pyspark.sql import DataFrame

from atc.etl import Loader, dataset_group
from atc.tables import TableHandle


class UpsertLoader(Loader):
    def __init__(
        self,
        handle: TableHandle,
        join_cols: List[str],
        dataset_input_keys: Union[str, List[str]] = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)

        self.handle = handle
        self.join_cols = join_cols

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        """Upserts a single dataframe to the target table."""
        self.handle.upsert(df=df, join_cols=self.join_cols)
