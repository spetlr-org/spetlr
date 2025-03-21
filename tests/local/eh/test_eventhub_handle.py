import json
from unittest.mock import patch

from pyspark.sql.types import (
    IntegerType,
    Row,
    StringType,
    StructField,
    StructType,
)
from spetlrtools.testing import DataframeTestCase

from spetlr.eh.eventhub_handle import EventhubHandle
from spetlr.exceptions import IncorrectSchemaException
from spetlr.spark import Spark


class KafkaEventhubHandleTest(DataframeTestCase):
    def test_kafka_config_construction(self):
        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
            maxEventsPerTrigger=50000,
        )

        self.assertEqual(
            handle.kafkaConfigs["kafka.bootstrap.servers"],
            "testNamespace.servicebus.windows.net:9093",
        )
        self.assertEqual(handle.kafkaConfigs["subscribe"], "testEventhub")
        self.assertEqual(handle.kafkaConfigs["kafka.group.id"], "testGroup")
        self.assertEqual(handle.kafkaConfigs["maxOffsetsPerTrigger"], "50000")

    def test_read_with_schema(self):
        schema = StructType([StructField("colA", IntegerType(), True)])
        json_data = json.dumps({"colA": 1}).encode()
        df = Spark.get().createDataFrame([(json_data,)], ["value"])

        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
            schema=schema,
        )

        with patch("pyspark.sql.DataFrameReader.load", return_value=df):
            result = handle.read()

        self.assertEqual(
            result.schema, StructType([StructField("value", schema, True)])
        )
        self.assertEqual(result.select("value").collect()[0][0], Row(colA=1))

    def test_read_without_schema(self):
        json_data = json.dumps({"colX": 1}).encode()
        df = Spark.get().createDataFrame([(json_data,)], ["value"])

        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
        )

        with patch("pyspark.sql.DataFrameReader.load", return_value=df):
            result = handle.read()

        self.assertEqual(
            result.schema, StructType([StructField("value", StringType(), True)])
        )
        self.assertEqual(result.select("value").collect()[0][0], '{"colX": 1}')

    def test_write_schema_validation(self):
        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
        )

        df = Spark.get().createDataFrame([("1", "2")], ["value", "something_else"])

        with self.assertRaises(IncorrectSchemaException):
            handle.append(df)

    def test_create_write_dataframe_output(self):
        schema = StructType([StructField("col1", StringType())])
        data = [("value1",), ("value2",)]
        df_to_write = Spark.get().createDataFrame(data, schema)

        handle = EventhubHandle(  # or EventhubHandle if still testing both
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
        )

        result = handle._create_write_dataframe(df_to_write)
        actual = [json.loads(row["value"]) for row in result.collect()]
        expected = [{"col1": "value1"}, {"col1": "value2"}]

        self.assertEqual(actual, expected)
