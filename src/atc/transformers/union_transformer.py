from functools import reduce

from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.etl.types import dataset_group


class UnionTransformer(Transformer):
    """
    This transformer unions multiple DataFrames

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

    def __init__(self, allowMissingColumns: bool = False):
        super().__init__()
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
