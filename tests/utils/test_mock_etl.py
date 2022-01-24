import unittest

from atc.etl import Transformer, Orchestrator
from atc.spark import Spark
from pyspark.sql import types, DataFrame

from atc.utils import DataframeCreator, MockExtractor, MockLoader


class MockEtlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Spark.get()

        cls.schema = types._parse_datatype_string(
            """
            Id INTEGER,
            measured DOUBLE
        """
        )

    def test_full_etl(self):
        df = DataframeCreator.make(self.schema, [(1, 3.5)])

        class MyTransformer(Transformer):
            def process(self, df: DataFrame) -> DataFrame:
                return df

        load = MockLoader()
        o = Orchestrator()
        o.extract_from(MockExtractor(df=df))
        o.transform_with(MyTransformer())
        o.load_into(load)
        o.execute()
        self.assertIs(df, load.getDf())

    def test_mocking(self):
        load = MockLoader()
        load.hello.world.foo.bar()
