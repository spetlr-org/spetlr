from typing import List, Union

from pyspark.sql import DataFrame

from atc.etl import TransformerNC


class SelectColumnsTransformerNC(TransformerNC):
    def __init__(
        self,
        *,
        columnList: List[str],
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
        )
        self.columnList = columnList

    def process(self, df: DataFrame) -> DataFrame:
        return df.select(self.columnList)
