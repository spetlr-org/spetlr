from pyspark.sql import DataFrame

from spetlr.etl import Extractor
from spetlr.tables import TableHandle


class StreamExtractor(Extractor):
    """This extractor will extract from any object that has a .read_stream() method."""

    def __init__(self, handle: TableHandle, dataset_key: str = None):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        return self.handle.read_stream()
