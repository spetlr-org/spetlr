import unittest
from datetime import datetime

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spetlr.azure_log_analytics import AzureLogAnalyticsHandle
from spetlr.functions import init_dbutils
from spetlr.spark import Spark


class TestAzureLogAnalyticsHandle(unittest.TestCase):
    def test_api_post(self):
        # fetching secrets that have been created by the deployment pipeline
        workspace_id = init_dbutils().secrets.get(
            "secrets", "LogAnalyticsWs--WorkspaceID"
        )

        primary_key = init_dbutils().secrets.get(
            "secrets", "LogAnalyticsWs--PrimaryKey"
        )

        az_lah = AzureLogAnalyticsHandle(
            log_analytics_workspace_id=workspace_id,
            shared_key=primary_key,
        )

        schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", IntegerType(), True),
                StructField("col_3", BooleanType(), True),
                StructField("col_4", TimestampType(), True),
            ]
        )

        data = [
            (
                "value",
                2903,
                True,
                datetime(2020, 2, 2),
            )
        ]

        spark = Spark().get()

        df = spark.createDataFrame(data, schema)

        status_code = az_lah.api_post(df)

        self.assertEqual(
            first=status_code,
            second=200,
        )
