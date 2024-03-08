from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.utils import DropOldestDuplicates


class DropOldestDuplicatesTransformer(Transformer):
    def __init__(
        self,
        *,
        cols: List[str],
        orderByColumn: str,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.cols = cols
        self.orderByColumn = orderByColumn

    def process(self, df: DataFrame) -> DataFrame:
        return DropOldestDuplicates(
            df=df, cols=self.cols, orderByColumn=self.orderByColumn
        )
