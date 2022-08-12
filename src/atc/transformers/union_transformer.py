from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.etl.types import dataset_group


class UnionTransformer(Transformer):
    """Returns the union of all input dataframes."""

    def process(self, df: DataFrame) -> DataFrame:
        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        dfs = list(datasets.values())
        df = dfs[0]
        for other in dfs[1:]:
            df = df.union(other)
        return df
