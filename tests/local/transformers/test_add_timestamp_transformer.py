import unittest
from datetime import datetime, timedelta

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import AddTimestampTransformer
from spetlr.spark import Spark


class AddTimestampTransformerTest(DataframeTestCase):
    def test_add_timestamp_transformer(self):
        """
        This test checks if a column is created with AddTimeStampTranformer,
        and check if ingestion time is within a few seconds of when the test has been run
        """
        input_schema = T.StructType(
            [
                T.StructField("Name", T.StringType(), True),
            ]
        )

        input_data = [("Arne",), ("Per",)]

        input_df = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        transformed_df = AddTimestampTransformer(col_name="Test_Timestamp").process(
            input_df
        )

        self.assertTrue("Test_Timestamp" in transformed_df.columns)

        now = datetime.now()

        transformer_timestamp = datetime.strptime(
            str(transformed_df.collect()[0][1]), "%Y-%m-%d %H:%M:%S.%f"
        )

        delta = transformer_timestamp - now

        self.assertTrue(delta < timedelta(seconds=2))


if __name__ == "__main__":
    unittest.main()
