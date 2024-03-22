from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class DropColumnsTransformer(Transformer):
    def __init__(
        self,
        *,
        columnList: List[str],
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.columnList = columnList

    def process(self, df: DataFrame) -> DataFrame:
        return df.drop(*self.columnList)
