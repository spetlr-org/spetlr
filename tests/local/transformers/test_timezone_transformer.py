from atc_tools.testing import DataframeTestCase
from pyspark.sql import types as T

from atc.spark import Spark
from atc.transformers.timezone_transformer import TimeZoneTransformer


class TimeZoneTransformerTest(DataframeTestCase):
    def test_timezone_transformer(self):
        input_schema = T.StructType(
            [
                T.StructField("Latitude", T.DoubleType(), True),
                T.StructField("Longitude", T.DoubleType(), True),
            ]
        )

        input_data = [(51.519487, -0.083069), (55.6761, 12.5683)]

        input_df = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        transformed_df = TimeZoneTransformer(
            latitude_col="Latitude",
            longitude_col="Longitude",
            dataset_input_key="None",
            dataset_output_key="None",
        ).process(input_df)

        expected_data = [
            (51.519487, -0.083069, "Europe/London"),
            (55.6761, 12.5683, "Europe/Copenhagen"),
        ]

        self.assertDataframeMatches(
            df=transformed_df,
            columns=None,
            expected_data=expected_data,
        )
