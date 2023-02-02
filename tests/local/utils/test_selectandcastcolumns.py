import unittest

import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase
from atc_tools.time import dt_utc

from atc.spark import Spark
from atc.utils import SelectAndCastColumns


class TestSelectAndCastColumns(DataframeTestCase):
    @classmethod
    def setUpClass(cls):
        if Spark._spark is None:
            Spark.config("spark.sql.shuffle.partitions", 1)
            Spark.master("local[1]")

    def test_select_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("value1", T.StringType(), True),
                T.StructField("value2", T.StringType(), True),
            ]
        )

        inputData = [
            (
                "value1",
                "value2",
            )
        ]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("value2", T.StringType(), True),
                T.StructField("value3", T.StringType(), True),
            ]
        )

        transformed_df = SelectAndCastColumns(df=input_df, schema=transformer_schema)

        expectedData = [
            (
                "value2",
                None,
            )
        ]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)

    def test_select_transformer_with_case_insensitive_matching(self):
        inputSchema = T.StructType(
            [
                T.StructField("VALUE1", T.StringType(), True),
                T.StructField("value2", T.StringType(), True),
            ]
        )

        inputData = [
            (
                "value1",
                "value2",
            )
        ]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("value1", T.StringType(), True),
                T.StructField("VALUE2", T.StringType(), True),
            ]
        )

        transformed_df = SelectAndCastColumns(
            df=input_df, schema=transformer_schema, caseInsensitiveMatching=True
        )

        expectedData = [
            (
                "value1",
                "value2",
            )
        ]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)

    def test_cast_transformer_valid_type_cast(self):
        datetime_to_use = dt_utc()

        inputSchema = T.StructType(
            [
                T.StructField("IntToStr", T.IntegerType(), True),
                T.StructField("StrToInt", T.StringType(), True),
                T.StructField("IntToDouble", T.IntegerType(), True),
                T.StructField("DoubleToInt", T.DoubleType(), True),
                T.StructField("IntToBooleanTrue", T.IntegerType(), True),
                T.StructField("IntToBooleanFalse", T.IntegerType(), True),
                T.StructField("BooleantoIntTrue", T.BooleanType(), True),
                T.StructField("BooleanToIntFalse", T.BooleanType(), True),
                T.StructField("StrToTimestamp", T.StringType(), True),
                T.StructField("TimestampToStr", T.TimestampType(), True),
            ]
        )

        inputData = [
            (
                42,
                "42",
                42,
                42.5,
                1,
                0,
                True,
                False,
                datetime_to_use.isoformat(),
                datetime_to_use,
            )
        ]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("IntToStr", T.StringType(), True),
                T.StructField("StrToInt", T.IntegerType(), True),
                T.StructField("IntToDouble", T.DoubleType(), True),
                T.StructField("DoubleToInt", T.IntegerType(), True),
                T.StructField("IntToBooleanTrue", T.BooleanType(), True),
                T.StructField("IntToBooleanFalse", T.BooleanType(), True),
                T.StructField("BooleantoIntTrue", T.IntegerType(), True),
                T.StructField("BooleanToIntFalse", T.IntegerType(), True),
                T.StructField("StrToTimestamp", T.TimestampType(), True),
                T.StructField("TimestampToStr", T.StringType(), True),
            ]
        )

        transformed_df = SelectAndCastColumns(df=input_df, schema=transformer_schema)

        expectedData = [
            (
                "42",
                42,
                42.0,
                42,
                True,
                False,
                1,
                0,
                datetime_to_use,
                datetime_to_use.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0"),
            )
        ]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)

    def test_cast_transformer_invalid_type_cast(self):
        inputSchema = T.StructType(
            [
                T.StructField("StrToInt", T.StringType(), True),
            ]
        )

        inputData = [("test",)]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("StrToInt", T.IntegerType(), True),
            ]
        )

        transformed_df = SelectAndCastColumns(df=input_df, schema=transformer_schema)

        expectedData = [(None,)]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)


if __name__ == "__main__":
    unittest.main()
