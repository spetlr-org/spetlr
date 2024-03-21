from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class FilterNullValuesTransformer(Transformer):
    """
    This transformer looks at a list of given columns and filter out data,
    where the columns has null values

    Parameters
    ----------
        columns : List[str]
            list of columns where null values are filtered out
    """

    def __init__(
        self,
        columns: List[str],
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.columns = columns

    def process(self, df: DataFrame) -> DataFrame:
        for column in self.columns:
            df = df.filter(F.col(column).isNotNull())

        return df
