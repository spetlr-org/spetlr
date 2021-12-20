import unittest

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Loader, DelegatingLoader
from atc.spark import Spark


class LoaderTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.loader = Loader()
        cls.df = create_dataframe()

    def test_save_returns_dataframe(self):
        self.assertEqual(self.loader.save(self.df), self.df)


class DelegatingLoaderTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.loaderList = [Loader(), Loader(), Loader()]
        cls.loader = DelegatingLoader(cls.loaderList)
        cls.df = create_dataframe()

    def test_get_loaders(self):
        self.assertEqual(self.loader.get_loaders(), self.loaderList)

    def test_save_returns_dataframe(self):
        self.assertEqual(self.loader.save(self.df), self.df)


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