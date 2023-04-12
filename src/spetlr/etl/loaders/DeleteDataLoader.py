from typing import Any, List, Union

from atc.etl import Loader, dataset_group
from atc.tables import TableHandle
from pyspark.sql import DataFrame


class DeleteDataLoader(Loader):
    def __init__(
        self,
        handle: TableHandle,
        comparison_col: str,
        comparison_limit: Any,
        comparison_operator: str = "<",
        dataset_input_keys: Union[str, List[str]] = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)

        self.handle = handle
        self.comparison_col = comparison_col
        self.comparison_limit = comparison_limit
        self.comparison_operator = comparison_operator

    def save_many(self, datasets: dataset_group) -> None:
        self.save(None)

    def save(self, df: DataFrame) -> None:
        """Delete old data from  a single dataframe to the target table."""
        self.handle.delete_data(
            self.comparison_col, self.comparison_limit, self.comparison_operator
        )
