import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from atc.etl import Transformer


class DataFrameFilterTransformer(Transformer):
    def __init__(self, col_value: str, col_name: str = "messageType") -> None:
        """
        A simple helper class to filter a dataframe based on the passed col_name
        if its value equals to the specified col_value.

        Args:
            col_value: The value of the column to filter the dataframe
            col_name: The name of the column to filter the dataframe. The default
                is 'messageType' as this class is mostly used to filter dataframes
                with this column name.
        """
        super().__init__()
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
