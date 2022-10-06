from functools import reduce
from typing import List

from pyspark.sql import DataFrame

from atc.etl import TransformerNC
from atc.etl.types import dataset_group


class UnionTransformerNC(TransformerNC):
    """
    This non-consuming transformer unions multiple DataFrames

    Attributes:
    ----------
        allowMissingColumns : bool
            When the parameter allowMissingColumns is True,
            the set of column names in this and other DataFrame
            can differ; missing columns will be filled with null

    Methods
    -------
    process(df: DataFrame):
        returns the input DataFrame

    process_many(datasets: dataset_group):
        returns the union of all input DataFrames
    """

    def __init__(
        self,
        dataset_input_key: str = None,
        dataset_input_key_list: List[str] = None,
        dataset_output_key: str = None,
        allowMissingColumns: bool = False,
    ):
        super().__init__(
            dataset_input_key=dataset_input_key,
            dataset_input_key_list=dataset_input_key_list,
            dataset_output_key=dataset_output_key,
        )
        self.allowMissingColumns = allowMissingColumns

    def process(self, df: DataFrame) -> DataFrame:
        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        dfs = list(datasets.values())
        return reduce(
            lambda df1, df2: df1.unionByName(
                df2, allowMissingColumns=self.allowMissingColumns
            ),
            dfs,
        )
