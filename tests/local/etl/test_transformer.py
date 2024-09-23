import unittest
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spetlrtools.testing import TestHandle

from spetlr.etl import Transformer
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.types import dataset_group
from spetlr.spark import Spark
from tests.local.etl.etl_transformer_test_helper import ETLTransformerTester


class TransformerTests(unittest.TestCase):
    """
    Test suite for the Transformer class in the OETL framework.

    This test suite contains test cases for both
        consuming and non-consuming transformers.
    """

    @classmethod
    def setUpClass(cls):
        cls.transformer_consuming = TestTransformer(consume_inputs=True)
        cls.transformer_non_consuming = TestTransformer(consume_inputs=False)
        cls.df = create_dataframe()

        # Test helpers for non consuming handling of keys
        cls.transformer_output_key = TestTransformer(
            dataset_output_key="my_output_key", consume_inputs=False
        )
        cls.transformer_input_key = TestTransformer(
            dataset_input_keys=["my_input_key"], consume_inputs=False
        )
        cls.transformer_input_key_list = TestTransformer(
            dataset_input_keys=["my_input_key_1", "my_input_key_2"],
            consume_inputs=False,
        )

    # ######################################################################
    # ################# CONSUMING TEST CASES ###############################
    # ######################################################################
    def test_consuming_process(self):
        # Process is called with single input
        # Result contain single df
        result = self.transformer_consuming.etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])

    def test_consuming_process_many(self):
        # Process_many is called with multiple inputs
        # Result contain union of input dfs
        result = self.transformer_consuming.etl({"df1": self.df, "df2": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertEqual(
            self.df.union(self.df).collect(), result["TestTransformer"].collect()
        )

    def test_consuming_dataset_key(self):
        # Set dataset_key to control name of the output
        # Result contain the key 'my_key'
        result = TestTransformer(dataset_key="my_key").etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"my_key"}, set(result.keys()))
        self.assertIs(self.df, result["my_key"])

    def test_consuming_dataset_output_key(self):
        # Set dataset_output_key to control name of the output
        # Result contain the key 'my_key'
        result = TestTransformer(dataset_output_key="my_key").etl({"df": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"my_key"}, set(result.keys()))
        self.assertIs(self.df, result["my_key"])

    def test_consuming_single_dataset_input_key(self):
        # Set dataset_input_keys to control what dfs in dataset to handle
        # Process is called with single input
        # Result contain two keys due to 'df2' not being consumed
        # when not in dataset_input_keys
        result = TestTransformer(dataset_input_keys=["df1"]).etl(
            {"df1": self.df, "df2": self.df}
        )
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df2"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])
        self.assertIs(self.df, result["df2"])

    def test_consuming_multiple_dataset_input_keys(self):
        # Set dataset_input_keys to control what dfs in dataset to handle
        # Process_many is called with multiple inputs
        # Result contain two keys due to 'df3' not being consumed
        # when not in dataset_input_keys
        result = TestTransformer(dataset_input_keys=["df1", "df2"]).etl(
            {"df1": self.df, "df2": self.df, "df3": self.df}
        )
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df3"}, set(result.keys()))
        self.assertEqual(
            self.df.union(self.df).collect(), result["TestTransformer"].collect()
        )
        self.assertIs(self.df, result["df3"])

    def test_consuming_should_not_consume_inputs(self):
        # Set consume_inputs to False to control dataset keys will no be consumed
        # in the output
        # Result contain two keys due to 'df' not being consumed
        result = TestTransformer(consume_inputs=False).etl({"df": self.df})
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])
        self.assertIs(self.df, result["df"])

    def test_consuming_in_etl_context(self):
        """
        In order to test whether the transformer class
        works in a full ETL flow,
        this tests a simple setup.

        It should be able to write df_trans to a testhandle.

        But cannot write df_1 to a testhandle, since it has been consumed.
        """
        trans_consuming = TestTransformer(
            dataset_input_keys=["df_1"],
            dataset_output_key="df_trans",
            consume_inputs=True,
        )

        oc_test_B = ETLTransformerTester(
            trans_consuming,
        )
        dh_load = TestHandle()

        oc_test_B.load_into(
            SimpleLoader(
                handle=dh_load, mode="overwrite", dataset_input_keys=["df_trans"]
            )
        )

        oc_test_B.execute()

        oc_test_B.load_into(
            SimpleLoader(handle=dh_load, mode="overwrite", dataset_input_keys=["df_1"])
        )

        with self.assertRaises(KeyError) as cm:
            oc_test_B.execute()

        # Since it is consuming, it should not be able to find df_1
        self.assertEqual(cm.exception.args[0], "df_1")

    # ######################################################################
    # ################# NON-CONSUMING TEST CASES ###########################
    # ######################################################################

    def test_non_consuming_process(self):
        # process is called when:
        # - inputs has one df
        # - dataset_input_key is not given
        result = self.transformer_non_consuming.etl({"df": self.df})
        self.assertIs(self.df, list(result.values())[1])

    def test_non_consuming_process_many(self):
        # process_many is called when:
        # - inputs has more than one df
        # - dataset_input_key is not given
        result = self.transformer_non_consuming.etl({"df1": self.df, "df2": self.df})
        self.assertEqual(6, list(result.values())[2].count())

    def test_non_consuming_process_no_key(self):
        # process sets dataset_ouput_key equivalent to the transformer class when:
        # - dataset_output_key is not given
        result = self.transformer_non_consuming.etl({"df": self.df})
        self.assertEqual("TestTransformer", list(result.keys())[1])

    def test_non_consuming_process_many_no_key(self):
        # process_many sets dataset_ouput_key equivalent to the transformer class when:
        # - dataset_output_key is not given
        result = self.transformer_non_consuming.etl({"df1": self.df, "df2": self.df})
        self.assertEqual("TestTransformer", list(result.keys())[2])

    def test_non_consuming_process_output_key(self):
        # process sets dataset_output_key equiavalent to the input when:
        # - dataset_output_key is given
        result = self.transformer_output_key.etl({"df": self.df})
        self.assertEqual("my_output_key", list(result.keys())[1])

    def test_non_consuming_process_many_output_key(self):
        # process_many sets dataset_output_key equiavalent to the input when:
        # - dataset_output_key is given
        result = self.transformer_output_key.etl({"df1": self.df, "df2": self.df})
        self.assertEqual("my_output_key", list(result.keys())[2])

    def test_non_consuming_process_input_key(self):
        # process is called when:
        # - dataset_input_key is given
        result = self.transformer_input_key.etl({"my_input_key": self.df})
        self.assertIs(self.df, list(result.values())[1])

        with self.assertRaises(KeyError):
            # error when:
            # - the provided dataset_input_key does not exist in inputs
            self.transformer_input_key.etl({"df": self.df})

    def test_non_consuming_process_many_input_key_list(self):
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

    def test_non_consuming_in_etl_context(self):
        """
        In order to test whether the transformer class
        works in a full ETL flow,
        this tests a simple setup.

        The non consuming can both write the df_1 and df_trans
        to a testhandle.
        """

        trans_consuming = TestTransformer(
            dataset_input_keys=["df_1"],
            dataset_output_key="df_trans",
            consume_inputs=False,
        )
        dh_load = TestHandle()
        oc_test_A = ETLTransformerTester(trans_consuming)
        oc_test_A.load_into(
            SimpleLoader(
                handle=dh_load, mode="overwrite", dataset_input_keys=["df_trans"]
            )
        )
        oc_test_A.load_into(
            SimpleLoader(handle=dh_load, mode="overwrite", dataset_input_keys=["df_1"])
        )

        oc_test_A.execute()


class TestTransformer(Transformer):
    def __init__(
        self,
        dataset_key: str = None,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_key=dataset_key,
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )

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
