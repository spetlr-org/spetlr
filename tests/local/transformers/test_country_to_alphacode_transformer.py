import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import CountryToAlphaCodeTransformer
from spetlr.spark import Spark


class CountryToAlphaCodeTransformerTest(DataframeTestCase):
    def test_country_to_alpha_code_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("countryCol", T.StringType(), True),
            ]
        )

        inputData = [
            ("Denmark",),
            ("Germany",),
        ]

        df_input = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [("DK",), ("DE",)]

        df_transformed = CountryToAlphaCodeTransformer(col_name="countryCol").process(
            df_input
        )

        self.assertDataframeMatches(df_transformed, None, expectedData)

    def test_country_to_alpha_code_transformer_new_col(self):
        inputSchema = T.StructType(
            [
                T.StructField("countryCol", T.StringType(), True),
            ]
        )

        inputData = [
            ("Denmark",),
            ("Germany",),
        ]

        df_input = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [
            (
                "Denmark",
                "DK",
            ),
            (
                "Germany",
                "DE",
            ),
        ]

        df_transformed = CountryToAlphaCodeTransformer(
            col_name="countryCol", output_col_name="alphaCodeCol"
        ).process(df_input)

        self.assertDataframeMatches(df_transformed, None, expectedData)

    def test_country_to_alpha_code_transformer_failure(self):
        inputSchema = T.StructType(
            [
                T.StructField("countryCol", T.StringType(), True),
            ]
        )

        inputData = [
            ("NotACountry",),
        ]

        df_input = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [
            ("",),
        ]

        df_transformed = CountryToAlphaCodeTransformer(col_name="countryCol").process(
            df_input
        )

        self.assertDataframeMatches(df_transformed, None, expectedData)
