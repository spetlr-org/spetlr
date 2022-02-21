import unittest
from unittest.mock import MagicMock

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Loader
from atc.spark import Spark


class LoaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):

        cls.loader = Loader()
        cls.loader.save = MagicMock()
        cls.loader.save_many = MagicMock()

        cls.df = create_dataframe()

    def test_save_single(self):

        input = {"myset": self.df}
        result = self.loader.etl(input)

        # assert that loader returns something:
        self.assertIs(input, result)

        args = self.loader.save.call_args[
            0
        ]  # the .args part became available in python 3.8
        print(args)
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df)

    def test_save_many(self):

        self.loader.etl({"myset1": self.df, "myset2": self.df})

        args = self.loader.save_many.call_args[
            0
        ]  # the .args part became available in python 3.8
        print(args)
        self.assertEqual(len(args), 1)
        datasets = args[0]
        self.assertEqual(len(datasets), 2)
        self.assertEqual(set(datasets.keys()), {"myset1", "myset2"})


def create_dataframe():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([(1, "1"), (2, "2"), (3, "3")]),
        StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("text", StringType(), False),
            ]
        ),
    )


if __name__ == "__main__":
    unittest.main()
