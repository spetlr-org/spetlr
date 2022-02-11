from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from atc.etl import Extractor


class MockExtractor(Extractor, MagicMock):
    def __init__(self, *args, dataset_key: str = None, df: DataFrame = None, **kwargs):
        Extractor.__init__(self, dataset_key=dataset_key)
        MagicMock.__init__(self, *args, **kwargs)
        self.df = df

    def read(self) -> DataFrame:
        return self.df
