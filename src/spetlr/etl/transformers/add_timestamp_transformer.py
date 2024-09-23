from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class AddTimestampTransformer(Transformer):
    """
    Adds a timestamp column for when a row was ingested

    Attributes:
    ----------
        col_name : str
            Name of timestamp column
        dataset_input_keys : Union[str, List[str]]
            list of input dataset keys
        dataset_output_key : str
            output dataset key
    """

    def __init__(
        self,
        col_name: str,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.col_name = col_name

    def process(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            self.col_name,
            F.current_timestamp(),
        )

        return df
