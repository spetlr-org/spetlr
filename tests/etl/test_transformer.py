import unittest
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Transformer, Orchestration
from atc.spark import Spark


class TransformerTests(unittest.TestCase):

    def test_process_returns_not_none(self):
        self.assertIsNotNone(Transformer().process(create_dataframe()))


class DelegatingTransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        orch = (Orchestration
                .extract_from(MagicMock())
                .transform_with(Transformer1())
                .transform_with(Transformer2())
                .transform_with(Transformer3())
                .load_into(MagicMock())
                .build()
                )

        cls.sut = sut = orch.transformer
        cls.df = sut.process(create_dataframe())

    def test_get_transformers_returns_not_none(self):
        self.assertIsNotNone(self.sut.get_transformers())

    def test_process_returns_dataframe(self):
        self.assertIsInstance(self.df, DataFrame)


def create_dataframe():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([
            (1, '1'),
            (2, '2'),
            (3, '3')
        ]),
        StructType([
            StructField("id", IntegerType(), False),
            StructField("text", StringType(), False)
        ]))


class Transformer1(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        return df


class Transformer2(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        return df


class Transformer3(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        return df
