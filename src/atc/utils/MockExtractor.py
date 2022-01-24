from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from atc.etl import Extractor


class MockExtractor(Extractor, MagicMock):
    def __init__(self, *args, dataset_key: str = None, df: DataFrame = None, **kwargs):
        super(Extractor, self).__init__(dataset_key=dataset_key)
        super(MagicMock, self).__init__(*args, **kwargs)
        self.df = df

    def read(self) -> DataFrame:
        return self.df
