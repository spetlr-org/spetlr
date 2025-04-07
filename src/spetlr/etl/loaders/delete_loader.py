from typing import List

from pyspark.sql import DataFrame

from spetlr.delta import DeltaHandle
from spetlr.etl import Loader


class DeleteLoader(Loader):
    def __init__(
        self,
        target_dh: DeltaHandle,
        join_cols: List[str] = None,
        dataset_input_key: str = None,
    ):
        """only a single dataset_input_key can be given
        An inner join will be carried out between the incoming dataframe and the target.
        The inner join will only consider the join_columns if given.
        All matched rows will be deleted from the target.
        """
        super().__init__(dataset_input_keys=[dataset_input_key])
        self.target_dh = target_dh
        self.join_cols = join_cols

    def save(self, df: DataFrame) -> None:
        self.target_dh.delete_rows(df, self.join_cols)
