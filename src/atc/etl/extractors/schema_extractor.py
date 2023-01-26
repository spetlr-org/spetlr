from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from atc.etl import Extractor
from atc.etl.extractors.simple_extractor import Readable


class SchemaExtractor(Extractor):
    """This extractor will extract from any object that has a .read() method,
    the resulting data frame will have zero rows and is intended for schema
    manipulation only."""

    def __init__(self, handle: Readable, dataset_key: str):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        df = self.handle.read()
        df = df.where(f.lit(False))
        return df
