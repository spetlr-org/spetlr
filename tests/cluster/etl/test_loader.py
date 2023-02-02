import unittest
from unittest.mock import MagicMock

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from atc.etl import Loader
from atc.spark import Spark


class LoaderTests(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.loader = Loader()
        self.loader.save = MagicMock()
        self.loader.save_many = MagicMock()

        self.df_1 = create_dataframe()
        self.df_2 = create_dataframe()
        self.df_3 = create_dataframe()

    def test_return_inputs(self):
        # Assert Loader returns ouput
        input = {"my_key_1": self.df_1}
        result = self.loader.etl(input)
        self.assertIs(input, result)

    def test_save(self):
        # save is called when:
        # - inputs has one df
        # - dataset_input_keys is not given
        self.loader.etl({"my_key_1": self.df_1})

        self.assertTrue(self.loader.save.called)
        self.assertFalse(self.loader.save_many.called)

        # the .args part became available in python 3.8
        args = self.loader.save.call_args[0]
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df_1)

    def test_save_many(self):
        # save is called when:
        # - inputs has more than one df
        # - dataset_input_keys is given
        self.loader.etl({"my_key_1": self.df_1, "my_key_2": self.df_2})

        self.assertFalse(self.loader.save.called)
        self.assertTrue(self.loader.save_many.called)

        # the .args part became available in python 3.8
        args = self.loader.save_many.call_args[0]
        self.assertEqual(len(args), 1)

        datasets = args[0]
        self.assertEqual(len(datasets), 2)
        self.assertEqual(set(datasets.keys()), {"my_key_1", "my_key_2"})
        self.assertEqual(set(datasets.values()), {self.df_1, self.df_2})

    def test_save_input_key(self):
        # save is called when:
        # - dataset_input_keys is str

        loader = Loader(dataset_input_keys="my_key_1")
        loader.save = MagicMock()
        loader.save_many = MagicMock()

        loader.etl({"my_key_1": self.df_1, "my_key_2": self.df_2})

        self.assertTrue(loader.save.called)
        self.assertFalse(loader.save_many.called)

        # the .args part became available in python 3.8
        args = loader.save.call_args[0]
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df_1)

    def test_save_single_input_key_list(self):
        # save is called when:
        # - dataset_input_keys has len 1

        loader = Loader(dataset_input_keys=["my_key_1"])
        loader.save = MagicMock()
        loader.save_many = MagicMock()

        loader.etl({"my_key_1": self.df_1, "my_key_2": self.df_2})

        self.assertTrue(loader.save.called)
        self.assertFalse(loader.save_many.called)

        # the .args part became available in python 3.8
        args = loader.save.call_args[0]
        self.assertEqual(len(args), 1)
        self.assertIs(args[0], self.df_1)

    def test_save_many_input_key_list(self):
        # save_many is called when:
        # - dataset_input_keys has len more than 1

        loader = Loader(dataset_input_keys=["my_key_1", "my_key_2"])
        loader.save = MagicMock()
        loader.save_many = MagicMock()

        loader.etl(
            {"my_key_1": self.df_1, "my_key_2": self.df_2, "my_key_3": self.df_3}
        )

        self.assertFalse(loader.save.called)
        self.assertTrue(loader.save_many.called)

        # the .args part became available in python 3.8
        args = loader.save_many.call_args[0]
        self.assertEqual(len(args), 1)

        datasets = args[0]
        self.assertEqual(len(datasets), 2)
        self.assertEqual(set(datasets.keys()), {"my_key_1", "my_key_2"})
        self.assertEqual(set(datasets.values()), {self.df_1, self.df_2})


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
