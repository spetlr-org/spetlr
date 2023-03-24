import unittest

import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.spark import Spark
from atc.transformers import SelectAndCastColumnsTransformerNC


class TestSelectAndCastColumnsTransformerNC(DataframeTestCase):
    def test_select_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("value", T.StringType(), True),
            ]
        )

        inputData = [
            (
                42,
                "data",
            )
        ]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("value2", T.StringType(), True),
            ]
        )

        transformed_df = SelectAndCastColumnsTransformerNC(
            schema=transformer_schema
        ).process(input_df)

        expectedData = [
            (
                42,
                None,
            )
        ]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)

    def test_cast_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("value", T.StringType(), True),
            ]
        )

        inputData = [
            (
                42,
                "data",
            )
        ]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        transformer_schema = T.StructType(
            [
                T.StructField("id", T.StringType(), True),
                T.StructField("value", T.StringType(), True),
            ]
        )

        transformed_df = SelectAndCastColumnsTransformerNC(
            schema=transformer_schema
        ).process(input_df)

        expectedData = [
            (
                "42",
                "data",
            )
        ]

        self.assertEqualSchema(transformed_df.schema, transformer_schema)
        self.assertDataframeMatches(transformed_df, None, expectedData)


if __name__ == "__main__":
    unittest.main()
