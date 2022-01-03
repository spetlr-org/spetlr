import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor
from atc.etl.extractor import DelegatingExtractor
from atc.spark import Spark


class ExtractorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        
        cls.df = create_dataframe()
        cls.extractor = TestExtractor1(cls.df)

    def test_read_returns_dataframe(self):
        self.assertEqual(self.extractor.read(), self.df)


class DelegatingExtractorTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df1 = create_dataframe()
        cls.df2 = create_dataframe()
        cls.df3 = create_dataframe()
        cls.extractorList = [TestExtractor1(cls.df1), TestExtractor2(cls.df2), TestExtractor3(cls.df3)]
        cls.extractor = DelegatingExtractor(cls.extractorList)

    def test_get_loaders(self):
        self.assertEqual(self.extractor.get_extractors(), self.extractorList)

    def test_return_returns_dictionary_of_dataframes(self):
        dataset = self.extractor.read()

        self.assertEqual(dataset.get(f'TestExtractor1'), self.df1)
        self.assertEqual(dataset.get(f'TestExtractor2'), self.df2)
        self.assertEqual(dataset.get(f'TestExtractor3'), self.df3)


class TestExtractor1(Extractor):
    def __init__(self, df: DataFrame):
        self.df = df

    def read(self):
        return self.df


class TestExtractor2(Extractor):
    def __init__(self, df: DataFrame):
        self.df = df

    def read(self):
        return self.df


class TestExtractor3(Extractor):
    def __init__(self, df: DataFrame):
        self.df = df

    def read(self):
        return self.df


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


if __name__ == "__main__":
    unittest.main()