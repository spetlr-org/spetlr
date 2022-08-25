from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.etl.types import dataset_group


class UnionTransformer(Transformer):
    """Returns the union of all input dataframes."""

    def __init__(self, allowMissingColumns=False):
        super().__init__()
        self.allowMissingColumns = allowMissingColumns

    def process(self, df: DataFrame) -> DataFrame:
        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        dfs = list(datasets.values())
        df = dfs[0]
        for other in dfs[1:]:
            if other is not None:
                df = df.unionByName(other, self.allowMissingColumns)
        return df
