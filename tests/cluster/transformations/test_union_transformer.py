import unittest

from pyspark.sql import DataFrame

from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.spark import Spark
from atc.transformers import UnionTransformer


class MergeDfIntoTargetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def test_01_union(self):
        schema = "i integer, s string"

        class Reader:
            df: DataFrame

            def read(self):
                return self.df

        th1 = Reader()
        th1.df = Spark.get().createDataFrame([(1, "a")], schema)

        th2 = Reader()
        th2.df = Spark.get().createDataFrame([(1, "a")], schema)

        class Writer:
            df: DataFrame

            def overwrite(self, df: DataFrame):
                self.df = df

        th3 = Writer()

        o = Orchestrator()
        o.extract_from(SimpleExtractor(th1, "th1"))
        o.extract_from(SimpleExtractor(th2, "th2"))
        o.transform_with(UnionTransformer())
        o.load_into(SimpleLoader(th3))
        o.execute()

        self.assertEqual(th1.df.schema, th3.df.schema)
