import unittest
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor, Orchestration, MultiInputTransformer
from atc.spark import Spark


class DelegatingExtractorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        class MyMultiInputTransformer(MultiInputTransformer):
            pass
        orch = (Orchestration
                .extract_from(Extractor1())
                .extract_from(Extractor2())
                .extract_from(Extractor3())
                .transform_with(MyMultiInputTransformer())
                .load_into(MagicMock())
                .build()
                )
        cls.sut = sut = orch.extractor
        cls.dataset = sut.read()

    def test_get_extractors_returns_not_none(self):
        self.assertIsNotNone(self.sut.get_extractors())

    def test_read_returns_dictionary_with_same_length_as_inner_extractors(self):
        self.assertEqual(len(self.dataset), len(self.sut.get_extractors()))

    def test_read_returns_dictionary_of_dataframes(self):
        for df in self.dataset.values():
            self.assertIsInstance(df, DataFrame)

    def test_read_returns_dictionary_with_extractors_type_name_as_keys(self):
        for x in range(1, 3):
            for df in self.dataset.get(f'Extractor{x}'):
                self.assertIsNotNone(df)


class Extractor1(Extractor):
    def read(self):
        return create_dataframe()


class Extractor2(Extractor):
    def read(self):
        return create_dataframe()


class Extractor3(Extractor):
    def read(self):
        return create_dataframe()


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
