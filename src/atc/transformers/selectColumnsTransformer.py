from typing import List

from pyspark.sql import DataFrame

from atc.etl import TransformerNC


class SelectColumnsTransformer(TransformerNC):
    def __init__(
        self,
        columnList: List[str],
        dataset_input_key: str = None,
        dataset_output_key: str = None,
    ):
        super().__init__(
            dataset_input_key=dataset_input_key, dataset_output_key=dataset_output_key
        )
        self.columnList = columnList

    def process(self, df: DataFrame) -> DataFrame:
        return df.select(self.columnList)
