import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.exceptions import ColumnDoesNotExistException, MoreThanTwoDataFramesException
from atc.spark import Spark
from atc.transformers import JoinDataframesTransformerNC


class TestJoinDataframesTransformerNC(DataframeTestCase):
    def test_data_join_inner(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="inner",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        transformed_data = [tuple(row) for row in df_transformed.collect()]

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            (None, None, "Id3", "Country3"),
        ]

        self.assertEqual(
            first=transformed_data,
            second=expected_data,
        )

    def test_data_join_left_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
            ("Id4", "Name4"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="left_outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            ("Id4", "Name4", None, None),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_right_outer(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="right_outer",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        transformed_data = [tuple(row) for row in df_transformed.collect()]

        expected_data = [
            (
                "Id1",
                "Name1",
                "Id1",
                "Country1",
            ),
            (
                "Id2",
                "Name2",
                "Id2",
                "Country2",
            ),
            (None, None, "Id3", "Country3"),
        ]

        self.assertEqual(
            first=transformed_data,
            second=expected_data,
        )

    def test_data_join_semi(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="semi",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [
            (
                "Id1",
                "Name1",
            ),
            (
                "Id2",
                "Name2",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_anti(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
            ("Id4", "Name4"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        df_transformed = JoinDataframesTransformerNC(
            first_dataframe_join_key="Id",
            second_dataframe_join_key="_Id",
            join_type="anti",
            dataset_input_keys=["input1_df", "input2_df"],
        ).process_many(dataset)

        expected_data = [("Id4", "Name4")]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_data_join_MoreThanTwoDataFramesException(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
            ("Id4", "Name4"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        input3_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Age", T.StringType(), True),
            ]
        )

        input3_data = [
            ("Id1", "Age1"),
            ("Id2", "Age2"),
            ("Id3", "Age3"),
        ]

        dataset["input3_df"] = Spark.get().createDataFrame(
            schema=input3_schema, data=input3_data
        )

        with self.assertRaises(MoreThanTwoDataFramesException):
            JoinDataframesTransformerNC(
                first_dataframe_join_key="Id",
                second_dataframe_join_key="_Id",
                join_type="anti",
                dataset_input_keys=["input1_df", "input2_df", "input3_df"],
            ).process_many(dataset)

    def test_data_join_ColumnDoesNotExistException(self):
        dataset = {}

        input1_schema = T.StructType(
            [
                T.StructField("Id", T.StringType(), True),
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input1_data = [
            ("Id1", "Name1"),
            ("Id2", "Name2"),
            ("Id4", "Name4"),
        ]

        dataset["input1_df"] = Spark.get().createDataFrame(
            schema=input1_schema, data=input1_data
        )

        input2_schema = T.StructType(
            [
                T.StructField("_Id", T.StringType(), True),
                T.StructField("Country", T.StringType(), True),
            ]
        )

        input2_data = [
            ("Id1", "Country1"),
            ("Id2", "Country2"),
            ("Id3", "Country3"),
        ]

        dataset["input2_df"] = Spark.get().createDataFrame(
            schema=input2_schema, data=input2_data
        )

        with self.assertRaises(ColumnDoesNotExistException):
            JoinDataframesTransformerNC(
                first_dataframe_join_key="Id_col",
                second_dataframe_join_key="_Id",
                join_type="anti",
                dataset_input_keys=["input1_df", "input2_df"],
            ).process_many(dataset)

        with self.assertRaises(ColumnDoesNotExistException):
            JoinDataframesTransformerNC(
                first_dataframe_join_key="Id",
                second_dataframe_join_key="_Id_col",
                join_type="anti",
                dataset_input_keys=["input1_df", "input2_df"],
            ).process_many(dataset)
