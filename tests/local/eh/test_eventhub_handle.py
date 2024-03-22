import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.eh.eventhub_handle import EventhubHandle
from spetlr.exceptions import IncorrectSchemaException, InvalidEventhubHandleParameters
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark


class EventhubHandleTest(DataframeTestCase):
    def test_create_with_connectionString(self):
        eventhubStreamExtractor = EventhubHandle(
            connection_str="testConnectionString",
            consumer_group="testConsumerGroup",
            maxEventsPerTrigger=100000,
            encrypt=False,
        )

        self.assertEqual(
            eventhubStreamExtractor.connectionString, "testConnectionString"
        )

    def test_create_without_connectionString(self):
        eventhubStreamExtractor = EventhubHandle(
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testAccessKeyName",
            accessKey="testAccessKey",
            consumer_group="testConsumerGroup",
            maxEventsPerTrigger=100000,
            encrypt=False,
        )

        self.assertEqual(
            eventhubStreamExtractor.connectionString,
            "Endpoint=sb://testNamespace.servicebus.windows.net/testEventhub;"
            "EntityPath=testEventhub;SharedAccessKeyName=testAccessKeyName;"
            "SharedAccessKey=testAccessKey",
        )

    def test_raise_create_exeption(self):
        with self.assertRaises(InvalidEventhubHandleParameters):
            EventhubHandle(
                connection_str=None,
                namespace=None,
                eventhub="testEventhub",
                accessKeyName="testAccessKeyName",
                accessKey="testAccessKey",
                consumer_group="testConsumerGroup",
                maxEventsPerTrigger=100000,
                encrypt=False,
            )

        with self.assertRaises(InvalidEventhubHandleParameters):
            EventhubHandle(
                connection_str=None,
                namespace="testNamespace",
                eventhub=None,
                accessKeyName="testAccessKeyName",
                accessKey="testAccessKey",
                consumer_group="testConsumerGroup",
                maxEventsPerTrigger=100000,
                encrypt=False,
            )

        with self.assertRaises(InvalidEventhubHandleParameters):
            EventhubHandle(
                connection_str=None,
                namespace="testNamespace",
                eventhub="testEventhub",
                accessKeyName=None,
                accessKey="testAccessKey",
                consumer_group="testConsumerGroup",
                maxEventsPerTrigger=100000,
                encrypt=False,
            )

        with self.assertRaises(InvalidEventhubHandleParameters):
            EventhubHandle(
                connection_str=None,
                namespace="testNamespace",
                eventhub="testEventhub",
                accessKeyName="testAccessKeyName",
                accessKey=None,
                consumer_group="testConsumerGroup",
                maxEventsPerTrigger=100000,
                encrypt=False,
            )

    def test_create_start_from_beginning_of_stream(self):
        eventhubStreamExtractor = EventhubHandle(
            connection_str="testConnectionString",
            consumer_group="testConsumerGroup",
            maxEventsPerTrigger=100000,
            encrypt=False,
        )

        expectedStartingEventPosition = {
            "offset": "-1",  # Start stream from beginning
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True,
        }

        self.assertEqual(
            eventhubStreamExtractor.startingEventPosition, expectedStartingEventPosition
        )

    def test_create_start_from_timestamp(self):
        timestampToUse = datetime.utcnow()

        eventhubStreamExtractor = EventhubHandle(
            connection_str="testConnectionString",
            consumer_group="testConsumerGroup",
            maxEventsPerTrigger=100000,
            startEnqueuedTime=timestampToUse,
            encrypt=False,
        )

        expectedStartingEventPosition = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": timestampToUse.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),  # Start from timestamp
            "isInclusive": True,
        }

        self.assertEqual(
            eventhubStreamExtractor.startingEventPosition, expectedStartingEventPosition
        )

    def test_from_tc(self):
        # Initialize the Configurator to manage test configurations
        tc = Configurator()
        # Clear any existing configurations to ensure a clean test environment
        tc.clear_all_configurations()
        # Enable debug mode
        tc.set_debug()

        # Define test values for event hub and consumer group
        eventhub = "spetlreh"
        consumer_group = "$Default"

        # Define a timestamp to use for the test
        timestampToUse = datetime(2020, 1, 1)
        _some_start_time = timestampToUse.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        # Register a test configuration with various parameters
        tc.register(
            "SpetlrEh",
            {
                "path": "/mnt/test/silver/spetlreh",
                "format": "avro",
                "partitioning": "ymd",
                "eh_eventhub": eventhub,
                "eh_consumer_group": consumer_group,
                "eh_connection_str": "testConnectionString",
                "eh_namespace": "testNamespace",
                "eh_accessKeyName": "testAccessKeyName",
                "eh_accessKey": "testAccessKey",
                "eh_maxEventsPerTrigger": "500000",
                "eh_startEnqueuedTime": _some_start_time,
                "schema": "EhSchema",
                "eh_encrypt": False,
            },
        )

        # Define an expected schema to be used in assertions later
        _expected_schema = StructType(
            [
                StructField("col_1", StringType(), True),
                StructField("col_2", IntegerType(), True),
                StructField("col_3", BooleanType(), True),
                StructField("col_4", TimestampType(), True),
            ]
        )

        # Register the expected schema with the SchemaManager
        SchemaManager().register_schema("EhSchema", _expected_schema)

        # Patch the 'read' method of EventhubHandle to not perform its actual function
        with patch.object(EventhubHandle, "read", return_value=None):
            # Use the class method 'from_tc' to create an instance of
            # EventhubHandle with the test configuration
            eh = EventhubHandle.from_tc("SpetlrEh")
            # Call the patched 'read' method
            eh.read()

            # Define the expected dictionary to verify eventhub configuration
            _expected_dict = {
                "eventhubs.connectionString": "testConnectionString",
                "eventhubs.consumerGroup": "$Default",
                "maxEventsPerTrigger": "500000",
                "eventhubs.startingPosition": "{"
                + f'"offset": null, "seqNo": -1, "enqueuedTime": '
                f'"{_some_start_time}", "isInclusive": true' + "}",
            }

            # Assert that the connectionString is as expected
            self.assertEqual(eh.connectionString, "testConnectionString")
            # Assert that the schema matches the expected schema
            self.assertEqual(eh._schema, _expected_schema)
            # Assert that the options dictionary matches the expected values
            self.assertEqual(eh.get_options_dict(), _expected_dict)

    def test_from_tc_with_explicit_connection_str(self):
        """
        Test the EventhubHandle.from_tc method with an explicit connection string.
        """
        # Initialize the Configurator to manage test configurations
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()

        # Register a minimal configuration for testing
        tc.register(
            "SpetlrEh",
            {
                "eh_connection_str": "explicitConnectionString",
                "eh_consumer_group": "$Default",
                "eh_eventhub": "testEventhub",
                "eh_namespace": "testNamespace",
                "eh_accessKeyName": "testAccessKeyName",
                "eh_accessKey": "testAccessKey",
                "eh_encrypt": False,
            },
        )

        # Define the explicit connection string to be used
        explicit_connection_str = "explicitConnectionStringOverride"

        # Patch the 'read' method of EventhubHandle to not perform its actual function
        with patch.object(EventhubHandle, "read", return_value=None):
            # Use the class method 'from_tc' to create an instance of EventhubHandle
            # with the test configuration, but override the connection string
            eh = EventhubHandle.from_tc(
                id="SpetlrEh", connection_str=explicit_connection_str
            )
            # Call the patched 'read' method
            eh.read()

            # Assert that the connectionString is overridden as expected
            self.assertEqual(eh.connectionString, explicit_connection_str)

    def test_wrong_from_tc_consumer_group(self):
        """
        In this test, the from_tc method misses a consumer group
        this throws an ValueError

        """
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()

        eventhub = "spetlreh"

        tc.register(
            "SpetlrEh",
            {
                "path": "/mnt/test/silver/spetlreh",
                "format": "avro",
                "partitioning": "ymd",
                "eh_eventhub": eventhub,
            },
        )

        with self.assertRaises(InvalidEventhubHandleParameters):
            with patch.object(EventhubHandle, "read", return_value=None):
                EventhubHandle.from_tc("SpetlrEh").read()

    def test_wrong_from_tc_maxtrigger(self):
        """
        In this test, the from_tc method have wrong type of max trigger

        """
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()

        eventhub = "spetlreh"
        consumer_group = "$Default"

        tc.register(
            "SpetlrEh",
            {
                "path": "/mnt/test/silver/spetlreh",
                "format": "avro",
                "partitioning": "ymd",
                "eh_eventhub": eventhub,
                "eh_consumer_group": consumer_group,
                "eh_maxEventsPerTrigger": "hellostring",
            },
        )
        with self.assertRaises(ValueError) as cm:
            with patch.object(EventhubHandle, "read", return_value=None):
                EventhubHandle.from_tc("SpetlrEh").read()

                self.assertIn(cm.exception.args[0], "hellostring")

    @patch("spetlr.spark.Spark.get")
    def test_eventhub_handle_init_encypt(self, mock_spark_get):
        # Setup the mock to have the chained methods ending with `encrypt`
        mock_encrypt = MagicMock(return_value="encrypted_connection_string")
        mock_jvm = MagicMock()
        mock_jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt = mock_encrypt
        mock_spark_context = MagicMock()
        mock_spark_context._jvm = mock_jvm
        mock_spark_session = MagicMock()
        mock_spark_session.sparkContext = mock_spark_context
        mock_spark_get.return_value = mock_spark_session

        # Instantiate your class (which should trigger the encryption)
        EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
        )

        # Assert that the encrypt method was called with the correct connection string
        mock_encrypt.assert_called_once_with(
            "Endpoint=sb://test_namespace.servicebus.windows.net/test_eventhub"
            ";EntityPath=test_eventhub"
            ";SharedAccessKeyName=test_key_name"
            ";SharedAccessKey=test_key"
        )

    @patch(target="pyspark.sql.DataFrameReader.load")
    def test_load_without_schema(self, mock_spark_load):

        eh = EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
            encrypt=False,
        )

        _something = json.dumps({"hey": "1"}).encode()
        df = Spark.get().createDataFrame([(_something,)], "body binary")

        mock_spark_load.return_value = df

        result = eh.read()

        self.assertEqual(
            result.schema, StructType([StructField("body", StringType(), True)])
        )
        self.assertEqual(result.select("body").collect()[0][0], '{"hey": "1"}')

    @patch(target="pyspark.sql.DataFrameReader.load")
    def test_load_with_schema(self, mock_spark_load):
        eh = EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
            encrypt=False,
            schema=StructType([StructField("colA", IntegerType(), True)]),
        )

        _something = json.dumps({"colA": 1}).encode()
        df = Spark.get().createDataFrame([(_something,)], "body binary")

        mock_spark_load.return_value = df

        result = eh.read()

        _body_struct = StructType([StructField("colA", IntegerType(), True)])
        self.assertEqual(
            result.schema, StructType([StructField("body", _body_struct, True)])
        )
        self.assertEqual(result.select("body").collect()[0][0], Row(colA=1))

    def test_incorrect_write_schema(self):
        eh = EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
            encrypt=False,
        )

        df = Spark.get().createDataFrame(
            [
                (
                    "1",
                    "2",
                )
            ],
            "body string, another string",
        )
        with self.assertRaises(IncorrectSchemaException):
            eh.append(df)

    def test_write_or_append_output_body(self):
        """
        This test ensures, that the dataframe
        is correctly outputted as json
        """

        # Create a test dataframe
        schema = StructType([StructField("col1", StringType())])
        data = [("value1",), ("value2",)]
        df_to_write = Spark.get().createDataFrame(data, schema)

        # Instantiate EventhubHandle
        eventhub_handle = EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
            encrypt=False,
        )

        result = eventhub_handle._create_write_dataframe(df_to_write)

        result.show()

        _data1 = '{"col1":"value1"}'
        _data2 = '{"col1":"value2"}'
        self.assertDataframeMatches(result, expected_data=[(_data1,), (_data2,)])

    def test_read_eventhub_stream(self):
        # Streaming locally is not testable at the moment
        pass


if __name__ == "__main__":
    unittest.main()
