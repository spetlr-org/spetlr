import unittest

from pyspark.sql import DataFrame

from atc.etl import Orchestrator, Transformer
from atc.schema_manager.spark_schema import get_schema
from atc.spark import Spark
from atc.utils import DataframeCreator, MockExtractor, MockLoader


class MockEtlTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Spark.get()

        cls.schema = get_schema(
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
