import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.spark import Spark
from spetlr.transformers import CountryToAlphaCodeTransformerNC


class CountryToAlphaCodeTransformer(DataframeTestCase):
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

        df_transformed = CountryToAlphaCodeTransformerNC(col_name="countryCol").process(
            df_input
        )

        self.assertDataframeMatches(df_transformed, None, expectedData)
