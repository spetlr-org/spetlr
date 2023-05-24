from typing import List, Union

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spetlr.etl import TransformerNC


class DataFrameFilterTransformerNC(TransformerNC):
    def __init__(
        self,
        col_name: str,
        col_value: str,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
    ) -> None:
        """

        The non-consuming version of DataFrameFilterTransformer()
        without a default column name.

        A simple helper class to filter a dataframe based on the passed col_name
        if its value equals to the specified col_value.

        Args:
            col_value: The value of the column to filter the dataframe
            col_name: The name of the column to filter the dataframe.
        """

        super().__init__(
            dataset_input_keys=dataset_input_keys, dataset_output_key=dataset_output_key
        )
        self.col_value = col_value
        self.col_name = col_name

    def process(self, df: DataFrame) -> DataFrame:
        """
        Method to filter the passed dataframe df based on the col_name and col_value
        of the class.

        Args:
            df: The dataframe to filter

        Returns:
            The filtered dataframe
        """
        return df.filter(F.col(self.col_name) == self.col_value)
