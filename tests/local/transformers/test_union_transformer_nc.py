import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.spark import Spark
from atc.transformers import UnionTransformerNC


class TestUnionTransformer(DataframeTestCase):
    def test_union_two_dataframes(self):
        input_schema = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
                T.StructField("Col5", T.StringType(), True),
            ]
        )

        input_data1 = [("Col1Data", 42, 13.37, "Col4Data", "Col5Data")]
        input_data2 = [("Col1Data_2nd", 42, 13.37, "Col4Data_2nd", "Col5Data_2nd")]

        input_df_1 = Spark.get().createDataFrame(data=input_data1, schema=input_schema)
        input_df_2 = Spark.get().createDataFrame(data=input_data2, schema=input_schema)
        input_datasets = {"DF1": input_df_1, "DF2": input_df_2}

        transformed_df = UnionTransformerNC().process_many(input_datasets)

        expected_data = [
            ("Col1Data", 42, 13.37, "Col4Data", "Col5Data"),
            ("Col1Data_2nd", 42, 13.37, "Col4Data_2nd", "Col5Data_2nd"),
        ]

        self.assertDataframeMatches(
            df=transformed_df,
            expected_data=expected_data,
        )

    def test_union_three_dataframes(self):
        input_schema = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
                T.StructField("Col5", T.StringType(), True),
            ]
        )

        input_data1 = [("Col1Data", 42, 13.37, "Col4Data", "Col5Data")]
        input_data2 = [("Col1Data_2nd", 42, 13.37, "Col4Data_2nd", "Col5Data_2nd")]
        input_data3 = [
            (
                "Totally new data",
                1234,
                98.76,
                "More totally new data",
                "Again, totally new data",
            )
        ]

        input_df_1 = Spark.get().createDataFrame(data=input_data1, schema=input_schema)
        input_df_2 = Spark.get().createDataFrame(data=input_data2, schema=input_schema)
        input_df_3 = Spark.get().createDataFrame(data=input_data3, schema=input_schema)
        input_datasets = {"DF1": input_df_1, "DF2": input_df_2, "DF3": input_df_3}

        transformed_df = UnionTransformerNC().process_many(input_datasets)

        expected_data = [
            ("Col1Data", 42, 13.37, "Col4Data", "Col5Data"),
            ("Col1Data_2nd", 42, 13.37, "Col4Data_2nd", "Col5Data_2nd"),
            (
                "Totally new data",
                1234,
                98.76,
                "More totally new data",
                "Again, totally new data",
            ),
        ]

        self.assertDataframeMatches(df=transformed_df, expected_data=expected_data)

    def test_union_dataframes_missing_columns(self):
        input_schema1 = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
                T.StructField("Col5", T.StringType(), True),
            ]
        )
        input_data1 = [
            (
                "Col1Data",
                42,
                13.37,
                "Col4Data",
                "Col5Data",
            )
        ]

        input_schema2 = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
            ]
        )
        input_data2 = [("Col1Data_2nd", 42, 13.37, "Col4Data_2nd")]

        input_df_1 = Spark.get().createDataFrame(data=input_data1, schema=input_schema1)
        input_df_2 = Spark.get().createDataFrame(data=input_data2, schema=input_schema2)
        input_datasets = {"DF1": input_df_1, "DF2": input_df_2}

        transformed_df = UnionTransformerNC(allowMissingColumns=True).process_many(
            input_datasets
        )

        expected_data = [
            ("Col1Data", 42, 13.37, "Col4Data", "Col5Data"),
            ("Col1Data_2nd", 42, 13.37, "Col4Data_2nd", None),
        ]

        self.assertDataframeMatches(df=transformed_df, expected_data=expected_data)
