import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from atc.etl import TransformerNC
from atc.etl.types import dataset_group
from atc.spark import Spark


class TransformerNCTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transformer = TestTransformer()
        cls.df = create_dataframe()

        cls.transformer_output_key = TestTransformer(dataset_output_key="my_output_key")
        cls.transformer_input_key = TestTransformer(dataset_input_keys="my_input_key")
        cls.transformer_input_key_list = TestTransformer(
            dataset_input_keys=["my_input_key_1", "my_input_key_2"]
        )

    def test_process(self):
        # process is called when:
        # - inputs has one df
        # - dataset_input_key is not given
        result = self.transformer.etl({"df": self.df})
        self.assertIs(self.df, list(result.values())[1])

    def test_process_many(self):
        # process_many is called when:
        # - inputs has more than one df
        # - dataset_input_key is not given
        result = self.transformer.etl({"df1": self.df, "df2": self.df})
        self.assertEqual(6, list(result.values())[2].count())

    def test_process_no_key(self):
        # process sets dataset_ouput_key equivalent to the transformer class when:
        # - dataset_output_key is not given
        result = self.transformer.etl({"df": self.df})
        self.assertEqual("TestTransformer", list(result.keys())[1])

    def test_process_many_no_key(self):
        # process_many sets dataset_ouput_key equivalent to the transformer class when:
        # - dataset_output_key is not given
        result = self.transformer.etl({"df1": self.df, "df2": self.df})
        self.assertEqual("TestTransformer", list(result.keys())[2])

    def test_process_output_key(self):
        # process sets dataset_output_key equiavalent to the input when:
        # - dataset_output_key is given
        result = self.transformer_output_key.etl({"df": self.df})
        self.assertEqual("my_output_key", list(result.keys())[1])

    def test_process_many_output_key(self):
        # process_many sets dataset_output_key equiavalent to the input when:
        # - dataset_output_key is given
        result = self.transformer_output_key.etl({"df1": self.df, "df2": self.df})
        self.assertEqual("my_output_key", list(result.keys())[2])

    def test_process_input_key(self):
        # process is called when:
        # - dataset_input_key is given
        result = self.transformer_input_key.etl({"my_input_key": self.df})
        self.assertIs(self.df, list(result.values())[1])

        with self.assertRaises(KeyError):
            # error when:
            # - the provided dataset_input_key does not exist in inputs
            self.transformer_input_key.etl({"df": self.df})

    def test_process_many_input_key_list(self):
        # process_many is called when:
        # - dataset_input_key_list is given
        result = self.transformer_input_key_list.etl(
            {"my_input_key_1": self.df, "my_input_key_2": self.df}
        )
        self.assertEqual(6, list(result.values())[2].count())

        with self.assertRaises(AssertionError):
            # error when:
            # - the provided dataset_input_key_list contains an incorrect key
            self.transformer_input_key_list.etl({"df1": self.df, "df2": self.df})


class TestTransformer(TransformerNC):
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
