import unittest
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spetlrtools.testing import TestHandle

from spetlr.etl import Orchestrator, Transformer
from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.loaders import SimpleLoader
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
        # Result contain union of input dfs
        result = self.transformer.etl({"df1": self.df, "df2": self.df})
        self.assertEqual(1, len(result))
        self.assertEqual({"TestTransformer"}, set(result.keys()))
        self.assertEqual(
            self.df.union(self.df).collect(), result["TestTransformer"].collect()
        )

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
        result = TestTransformer(dataset_input_keys=["df1"]).etl(
            {"df1": self.df, "df2": self.df}
        )
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df2"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])
        self.assertIs(self.df, result["df2"])

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
        self.assertEqual(
            self.df.union(self.df).collect(), result["TestTransformer"].collect()
        )
        self.assertIs(self.df, result["df3"])

    def test_not_consume_inputs(self):
        # Set consume_inputs to False to control dataset keys will no be consumed
        # in the output
        # Result contain two keys due to 'df' not being consumed
        result = TestTransformer(consume_inputs=False).etl({"df": self.df})
        self.assertEqual(2, len(result))
        self.assertEqual({"TestTransformer", "df"}, set(result.keys()))
        self.assertIs(self.df, result["TestTransformer"])
        self.assertIs(self.df, result["df"])

    def test_non_consume_in_etl_context(self):
        """
        In order to test whether the transformer class
        works in a full ETL flow,
        this tests a simple setup
        """

        trans_consuming = TestTransformer2(
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

        oc_test_A.execute()

    def test_consume_in_etl_context(self):
        """
        In order to test whether the transformer class
        works in a full ETL flow,
        this tests a simple setup
        """
        trans_consuming = TestTransformer2(
            dataset_input_keys=["df_1"],
            dataset_output_key="df_trans",
        )

        oc_test_B = ETLTransformerTester(
            trans_consuming,
        )
        dh_load = TestHandle()
        oc_test_B.load_into(
            SimpleLoader(handle=dh_load, mode="overwrite", dataset_input_keys=["df_1"])
        )

        with self.assertRaises(KeyError) as cm:
            oc_test_B.execute()

        # Since it is consuming, it should not be able to find df_1
        self.assertEqual(cm.exception.args[0], "df_1")


class TestTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        assert len(datasets) == 2
        df1, df2 = list(datasets.values())
        return df1.union(df2)


class TestTransformer2(Transformer):
    def __init__(
        self,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )

    def process(self, df: DataFrame) -> DataFrame:
        return df


def create_dataframe():
    data = [(1, "1"), (2, "2"), (3, "3")]

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("text", StringType(), False),
        ]
    )

    return Spark.get().createDataFrame(data=data, schema=schema)


def ETLTransformerTester(trans) -> Orchestrator:
    empty_df = Spark.get().createDataFrame(data=[], schema="col1 string")

    dh_extract_1 = TestHandle(provides=empty_df)
    dh_extract_2 = TestHandle(provides=empty_df)

    oc = Orchestrator()

    oc.extract_from(SimpleExtractor(handle=dh_extract_1, dataset_key="df_1"))

    oc.extract_from(SimpleExtractor(handle=dh_extract_2, dataset_key="df_2"))

    oc.transform_with(
        trans,
    )

    return oc


if __name__ == "__main__":
    unittest.main()
