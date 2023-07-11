import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.etl import Transformer
from spetlr.etl.types import dataset_group
from spetlr.spark import Spark


class TransformerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.transformer = TestTransformer()
        cls.df = create_dataframe()

    def test_process(self):
        # Process is called with single input
        # Result contain single df
        result = self.transformer.etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])

    def test_process_many(self):
        # Process_many is called with multiple inputs
        # Result contain union of input dfs og length 6
        result = self.transformer.etl({"df1": self.df, "df2": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertEqual(6, result["TestTransformer"].count())

    def test_dataset_key(self):
        # Set dataset_key to control name of the output
        # Result contain the key 'my_key'
        result = TestTransformer(dataset_key="my_key").etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"my_key"}, set(result.keys()))
        self.assertIs(self.df, result["my_key"])

    def test_dataset_output_key(self):
        # Set dataset_output_key to control name of the output
        # Result contain the key 'my_key'
        result = TestTransformer(dataset_output_key="my_key").etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"my_key"}, set(result.keys()))
        self.assertIs(self.df, result["my_key"])

    def test_single_dataset_input_key(self):
        # Set dataset_input_keys to control what dfs in dataset to handle
        # Process is called with single input
        # Result contain two keys due to 'df2' not being consumed
        # when not in dataset_input_keys
        result = TestTransformer(dataset_input_keys="df1").etl(
            {"df1": self.df, "df2": self.df}
        )
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df2"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])

    def test_multiple_dataset_input_keys(self):
        # Set dataset_input_keys to control what dfs in dataset to handle
        # Process_many is called with multiple inputs
        # Result contain two keys due to 'df3' not being consumed
        # when not in dataset_input_keys
        result = TestTransformer(dataset_input_keys=["df1", "df2"]).etl(
            {"df1": self.df, "df2": self.df, "df3": self.df}
        )
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df3"}, set(result.keys()))
        self.assertEqual(6, result["TestTransformer"].count())

    def test_not_consume_inputs(self):
        # Set consume_inputs to False to control dataset keys will no be consumed
        # in the output
        # Result contain two keys due to 'df' not being consumed
        result = TestTransformer(consume_inputs=False).etl({"df": self.df})
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])


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
