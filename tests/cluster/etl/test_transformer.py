import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from atc.etl import Transformer
from atc.etl.types import dataset_group
from atc.spark import Spark


class TransformerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transformer = TestTransformer()
        cls.df = create_dataframe()

    def test_process_returns_dataframe(self):
        result = self.transformer.etl({"df": self.df})
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertIs(list(result.values())[0], self.df)

    def test_process_many(self):
        result = self.transformer.etl({"df1": self.df, "df2": self.df})
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertEqual(6, list(result.values())[0].count())

    def test_dataset_key(self):
        result = TestTransformer("my_key").etl({"df": self.df})
        self.assertEqual({"my_key"}, set(result.keys()))


class TestTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        assert len(datasets) == 2
        df1, df2 = list(datasets.values())
        return df1.union(df2)


def create_dataframe():
    data = [(1, "1"), (2, "2"), (3, "3")]

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("text", StringType(), False),
        ]
    )

    return Spark.get().createDataFrame(data=data, schema=schema)


if __name__ == "__main__":
    unittest.main()
