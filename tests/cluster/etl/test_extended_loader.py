import unittest
from unittest.mock import MagicMock

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from atc.etl import ExtendedLoader
from atc.spark import Spark


class LoaderTests(unittest.TestCase):
    def setUp(self):

        self.loader = ExtendedLoader()
        self.loader.save = MagicMock()
        self.loader.save_many = MagicMock()

        self.loader_input_key = ExtendedLoader(dataset_input_key="my_input_key")
        self.loader_input_key.save = MagicMock()
        self.loader_input_key.save_many = MagicMock()

        self.dataset_input_key_list = ["my_input_key_1", "my_input_key_2"]
        self.loader_input_key_list = ExtendedLoader(
            dataset_input_key_list=self.dataset_input_key_list
        )
        self.loader_input_key_list.save = MagicMock()
        self.loader_input_key_list.save_many = MagicMock()

        self.df = create_dataframe()
        self.df_empty = create_empty_dataframe()

    def test_return_inputs(self):
        # loader returns ouput
        input = {"df": self.df}
        result = self.loader.etl(input)
        self.assertIs(input, result)

    def test_save(self):
        # save is called when:
        # - inputs has one df
        # - dataset_input_key is not given
        self.loader.etl({"df": self.df})
        self.assertTrue(self.loader.save.called)
        self.assertFalse(self.loader.save_many.called)

    def test_save_many(self):
        # save_many is called when:
        # - inputs has more than one df
        # - dataset_input_key is not given
        self.loader.etl({"df1": self.df, "df2": self.df})
        self.assertFalse(self.loader.save.called)
        self.assertTrue(self.loader.save_many.called)

    def test_save_input_key(self):
        # save is called when:
        # - inputs has more than one df
        # - dataset_input_key is given
        self.loader_input_key.etl({"df": self.df_empty, "my_input_key": self.df})
        self.assertTrue(self.loader_input_key.save.called)
        self.assertFalse(self.loader_input_key.save_many.called)
        self.assertIs(self.loader_input_key.save.call_args[0][0], self.df)

    def test_save_many_input_key_list(self):
        # save_many is called when:
        # - inputs has more than one df
        # - dataset_input_key_list is given
        self.loader_input_key_list.etl(
            {"df": self.df_empty, "my_input_key_1": self.df, "my_input_key_2": self.df}
        )
        self.assertFalse(self.loader_input_key_list.save.called)
        self.assertTrue(self.loader_input_key_list.save_many.called)

        for input_key in self.dataset_input_key_list:
            self.assertTrue(
                input_key
                in dict(self.loader_input_key_list.save_many.call_args[0][0]).keys()
            )


def create_dataframe():
    data = [(1, "1"), (2, "2"), (3, "3")]

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("text", StringType(), False),
        ]
    )

    return Spark.get().createDataFrame(data=data, schema=schema)


def create_empty_dataframe():
    data = [()]
    return Spark.get().createDataFrame(data=data)


if __name__ == "__main__":
    unittest.main()
