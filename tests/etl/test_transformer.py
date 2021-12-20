import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Transformer, DelegatingTransformer, MultiInputTransformer, DelegatingMultiInputTransformer
from atc.spark import Spark


class TransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        transformer = Transformer1()
        cls.df = transformer.process(create_dataframe())

    def test_process_returns_dataframe(self):
        self.assertIsInstance(self.df, DataFrame)


class DelegatingTransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.transformerList = [Transformer1(), Transformer2(), Transformer3()]
        cls.transformer = DelegatingTransformer(cls.transformerList)
        cls.df = cls.transformer.process(create_dataframe())

    def test_get_transformers(self):
        self.assertEqual(self.transformer.get_transformers(), self.transformerList)

    def test_process_returns_dataframe(self):
        self.assertIsInstance(self.df, DataFrame)


class MultiInputTransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        transformer = MultiInputTransformer1()
        dataset = {
            "df1": create_dataframe(),
            "df2": create_dataframe()
        }
        cls.df = transformer.process_many(dataset)

    def test_process_returns_dataframe(self):
        self.assertIsInstance(self.df, DataFrame)


class DelegatingMultiInputTransformerTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.transformerList = [MultiInputTransformer1(), Transformer1(), Transformer2(), Transformer3()]
        cls.transformer = DelegatingMultiInputTransformer(cls.transformerList)
        dataset = {
            "df1": create_dataframe(),
            "df2": create_dataframe()
        }
        cls.df = cls.transformer.process_many(dataset)

    def test_get_transformers(self):
        self.assertEqual(self.transformer.get_transformers(), self.transformerList)

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

class MultiInputTransformer1(MultiInputTransformer):
    def process_many(self, dataset):
        df1 = dataset["df1"]
        df2 = dataset["df2"]
        return df1.union(df2)

class Transformer1(Transformer):
    def process(self, df):
        return df


class Transformer2(Transformer):
    def process(self, df):
        return df


class Transformer3(Transformer):
    def process(self, df):
        return df

if __name__ == "__main__":
    unittest.main()
