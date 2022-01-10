import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor
from atc.etl.types import dataset_group
from atc.spark import Spark


class ExtractorTests(unittest.TestCase):

    df: DataFrame = None

    @classmethod
    def setUpClass(cls):

        cls.df = create_dataframe()
        cls.extractor = MyTestExtractor1(cls.df)
        cls.keyed_extractor = MyTestExtractor2(cls.df)
        cls.doubling_extractor = MyTestExtractor3()

    def test_read_returns_dataframe(self):
        self.assertEqual(self.extractor.read(), self.df)

    def test_dataset_names(self):
        self.assertEqual(list(self.extractor.etl({}).keys()), ["MyTestExtractor1"])

    def test_dataset_names_keyed(self):
        self.assertEqual(list(self.keyed_extractor.etl({}).keys()), ["mykey"])

    def test_dataset_access(self):
        steps = [self.extractor, self.doubling_extractor]
        datasets: dataset_group = {}
        for step in steps:
            datasets = step.etl(datasets)

        self.assertEqual(set(datasets.keys()), {"MyTestExtractor1", "MyTestExtractor3"})
        df1, df3 = list(datasets.values())
        self.assertIs(self.df, df1)
        self.assertIs(self.df, df3)


class MyTestExtractor1(Extractor):
    def __init__(self, df: DataFrame):
        super().__init__()
        self.df = df

    def read(self):
        return self.df


class MyTestExtractor2(Extractor):
    def __init__(self, df: DataFrame):
        super().__init__(dataset_key="mykey")
        self.df = df

    def read(self):
        return self.df


class MyTestExtractor3(Extractor):
    """Doubles the input dataframe"""

    def read(self):
        assert len(self.previous_extractions) == 1
        # pick out the other df and return it as your own
        (df,) = list(self.previous_extractions.values())
        return df


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
