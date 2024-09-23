from typing import Protocol

from pyspark.sql import DataFrame

from spetlr.etl.extractors.lazy_extractor import LazyExtractor


class Readable(Protocol):
    def read(self) -> DataFrame:
        pass


class LazySimpleExtractor(LazyExtractor):
    """This extractor will extract from any object that has a .read() method."""

    def __init__(self, handle: Readable, dataset_key: str = None):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        return self.handle.read()
