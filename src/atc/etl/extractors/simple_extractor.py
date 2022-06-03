from typing import Protocol

from pyspark.sql import DataFrame

from atc.etl import Extractor


class Readable(Protocol):
    def read(self) -> DataFrame:
        pass


class SimpleExtractor(Extractor):
    """This extractor will extract from any object that has a .read() method."""

    def __init__(self, handle: Readable, dataset_key: str):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        return self.handle.read()
