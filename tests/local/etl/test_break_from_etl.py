import unittest
from typing import Optional

from pyspark.sql import DataFrame

from spetlr.etl import Extractor, Loader, Orchestrator, Transformer
from spetlr.spark import Spark


class BreakFromEtlTests(unittest.TestCase):
    def test_without_break(self):
        with self.assertRaises(NotImplementedError):
            BreakTestOrchestrator(NoBreakExtractor()).execute()

    def test_with_break(self):
        BreakTestOrchestrator(BreakExtractor()).execute()
        self.assertTrue(True)


class NoBreakExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame([], schema="Id INTEGER")


class BreakExtractor(Extractor):
    def read(self) -> Optional[DataFrame]:
        return None


class BreakTestOrchestrator(Orchestrator):
    def __init__(self, extractor: Extractor):
        super().__init__()
        self.extract_from(extractor)
        self.transform_with(Transformer())
        self.load_into(Loader())
