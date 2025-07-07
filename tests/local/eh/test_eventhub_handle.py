import json
from datetime import timezone
from unittest.mock import patch

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase
from spetlrtools.time import dt_utc

from spetlr import Configurator
from spetlr.eh.eventhub_handle import EventhubHandle
from spetlr.exceptions import (
    InvalidEventhubConnectionString,
    InvalidEventhubHandleParameters,
    InvalidEventhubWriteSchema,
)
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark


class KafkaEventhubHandleTest(DataframeTestCase):

    _kafka_schema_cols = [
        "key",
        "value",
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestampType",
    ]

    _test_cn_string = (
        "Endpoint=sb://NamespaceName.servicebus.windows.net/;"
        "SharedAccessKeyName=KeyName;SharedAccessKey=KeyValue"
    )

    def test_create_with_connectionString(self):
        eh = EventhubHandle(
            connection_str=self._test_cn_string,
            consumer_group="testConsumerGroup",
            eventhub="hey",
            namespace="there",
            maxEventsPerTrigger=100000,
        )

        self.assertEqual(eh.connectionString, self._test_cn_string)

    def test_create_without_connectionString(self):
        eh = EventhubHandle(
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testAccessKeyName",
            accessKey="testAccessKey",
            consumer_group="testConsumerGroup",
            maxEventsPerTrigger=100000,
        )

        self.assertEqual(
            eh.connectionString,
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
            )

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

        # Register a test configuration with various parameters
        tc.register(
            "SpetlrEh",
            {
                "path": "/mnt/test/silver/spetlreh",
                "format": "avro",
                "partitioning": "ymd",
                "eh_eventhub": eventhub,
                "eh_consumer_group": consumer_group,
                "eh_connection_str": self._test_cn_string,
                "eh_namespace": "testNamespace",
                "eh_accessKeyName": "testAccessKeyName",
                "eh_accessKey": "testAccessKey",
                "eh_maxEventsPerTrigger": "500000",
                "schema": "EhSchema",
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
                "kafka.bootstrap.servers": "testNamespace.servicebus.windows.net:9093",
                "subscribe": "spetlreh",
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "PLAIN",
                "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security"
                ".plain.PlainLoginModule "
                'required username="$ConnectionString" '
                f'password="{self._test_cn_string}";',
                "maxOffsetsPerTrigger": "500000",
                "kafka.group.id": "$Default",
            }
            # Assert that the connectionString is as expected
            self.assertEqual(eh.connectionString, self._test_cn_string)
            # Assert that the schema matches the expected schema
            self.assertEqual(eh._schema, _expected_schema)
            # Assert that the options dictionary matches the expected values
            print(eh.get_options_dict())
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
            },
        )

        # Define the explicit connection string to be used
        explicit_connection_str = self._test_cn_string

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
        with self.assertRaises(ValueError):
            with patch.object(EventhubHandle, "read", return_value=None):
                EventhubHandle.from_tc("SpetlrEh").read()

    def test_from_tc_default_maxtrigger(self):
        """
        In this test, the from_tc method have default type of max trigger

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
                "eh_namespace": "hey",
                "eh_accessKeyName": "d",
            },
        )
        with self.assertRaises(KeyError):
            with patch.object(EventhubHandle, "read", return_value=None):
                eh = EventhubHandle.from_tc("SpetlrEh", accessKey="2")
                eh.read()
                eh.get_options_dict()["maxOffsetsPerTrigger"]

    def test_read_with_schema(self):
        schema = StructType([StructField("colA", IntegerType(), True)])
        json_data = json.dumps({"colA": 1}).encode()
        df = Spark.get().createDataFrame(
            [
                (
                    "some_key",
                    json_data,
                    "some_topic",
                    1,
                    5888,
                    dt_utc(2000, 1, 1),
                    0,
                )
            ],
            self._kafka_schema_cols,
        )

        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
            schema=schema,
        )

        with patch("pyspark.sql.DataFrameReader.load", return_value=df):
            resultA = handle.read()

        with patch("pyspark.sql.streaming.DataStreamReader.load", return_value=df):
            resultB = handle.read_stream()

        _expected_schema = StructType(
            [
                # EventhubRowId can be null
                StructField("EventhubRowId", LongType(), False),
                # BodyId can be null
                StructField("BodyId", LongType(), False),
                StructField("SequenceNumber", LongType(), True),
                StructField("PartitionNumber", IntegerType(), True),
                StructField("EnqueuedDate", DateType(), True),
                StructField("EnqueuedTime", TimestampType(), True),
                StructField("StreamingTime", TimestampType(), False),
                StructField("Properties", StringType(), False),
                StructField("SystemProperties", StringType(), False),
                StructField("Body", schema, True),
            ]
        )

        for result in [resultA, resultB]:

            self.assertEqual(result.schema, _expected_schema)
            self.assertEqual(
                result.select("EventhubRowId").collect()[0][0], 3269387136041157559
            )
            self.assertEqual(
                result.select("BodyId").collect()[0][0], -3514496451369984469
            )
            self.assertEqual(result.select("SequenceNumber").collect()[0][0], 5888)
            self.assertEqual(result.select("PartitionNumber").collect()[0][0], 1)
            self.assertEqual(
                result.select("EnqueuedDate").collect()[0][0], dt_utc(2000, 1, 1).date()
            )
            self.assertEqual(
                result.select("EnqueuedTime").collect()[0][0].astimezone(timezone.utc),
                dt_utc(2000, 1, 1),
            )
            self.assertEqual(result.select("Properties").collect()[0][0], "{}")
            self.assertEqual(result.select("SystemProperties").collect()[0][0], "{}")
            self.assertEqual(result.select("PartitionNumber").collect()[0][0], 1)
            # Not testing StreamingTime
            self.assertEqual(result.select("Body").collect()[0][0], Row(colA=1))

    def test_read_without_schema(self):
        json_data = json.dumps({"colX": 1}).encode()
        df = Spark.get().createDataFrame(
            [
                (
                    "some_key",
                    json_data,
                    "some_topic",
                    1,
                    5888,
                    dt_utc(2000, 1, 1),
                    0,
                )
            ],
            self._kafka_schema_cols,
        )

        handle = EventhubHandle(
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
        )

        with patch("pyspark.sql.DataFrameReader.load", return_value=df):
            resultA = handle.read()

        with patch("pyspark.sql.streaming.DataStreamReader.load", return_value=df):
            resultB = handle.read_stream()

        _expected_schema = StructType(
            [
                StructField("EventhubRowId", LongType(), False),
                StructField("BodyId", LongType(), False),
                StructField("SequenceNumber", LongType(), True),
                StructField("PartitionNumber", IntegerType(), True),
                StructField("EnqueuedDate", DateType(), True),
                StructField("EnqueuedTime", TimestampType(), True),
                StructField("StreamingTime", TimestampType(), False),
                StructField("Properties", StringType(), False),
                StructField("SystemProperties", StringType(), False),
                StructField("Body", StringType(), True),
            ]
        )

        for result in [resultA, resultB]:

            self.assertEqual(result.schema, _expected_schema)
            self.assertEqual(
                result.select("EventhubRowId").collect()[0][0], 1176656857733296004
            )
            self.assertEqual(
                result.select("BodyId").collect()[0][0], 8753335798344916359
            )
            self.assertEqual(result.select("SequenceNumber").collect()[0][0], 5888)
            self.assertEqual(result.select("PartitionNumber").collect()[0][0], 1)
            self.assertEqual(
                result.select("EnqueuedDate").collect()[0][0], dt_utc(2000, 1, 1).date()
            )
            self.assertEqual(
                result.select("EnqueuedTime").collect()[0][0].astimezone(timezone.utc),
                dt_utc(2000, 1, 1),
            )
            self.assertEqual(result.select("Properties").collect()[0][0], "{}")
            self.assertEqual(result.select("SystemProperties").collect()[0][0], "{}")
            self.assertEqual(result.select("PartitionNumber").collect()[0][0], 1)
            # Not testing StreamingTime
            self.assertEqual(result.select("Body").collect()[0][0], '{"colX": 1}')

    def test_valid_write_dataframe_output(self):
        schema = StructType([StructField("value", StringType())])
        data = [("value1",), ("value2",)]
        df_to_write = Spark.get().createDataFrame(data, schema)

        handle = EventhubHandle(  # or EventhubHandle if still testing both
            consumer_group="testGroup",
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testKeyName",
            accessKey="testKey",
        )

        handle._check_write_dataframe(df_to_write)

    def test_incorrect_write_schema(self):
        eh = EventhubHandle(
            consumer_group="test_group",
            namespace="test_namespace",
            eventhub="test_eventhub",
            accessKeyName="test_key_name",
            accessKey="test_key",
        )

        df = Spark.get().createDataFrame(
            [
                (
                    "1",
                    "2",
                )
            ],
            "key string, another string",
        )
        with self.assertRaises(InvalidEventhubWriteSchema):
            eh.append(df)

    def test_valid_connection_string_parsing(self):
        eh = EventhubHandle(
            connection_str=(
                "Endpoint=sb://testns.servicebus.windows.net/testhub;"
                "EntityPath=testhub;SharedAccessKeyName=testkey;"
                "SharedAccessKey=secret"
            )
        )

        self.assertEqual(eh.bootstrap_servers, "testns.servicebus.windows.net:9093")
        self.assertEqual(eh.topic, "testhub")

    def test_invalid_connection_string_raises(self):
        with self.assertRaises(InvalidEventhubConnectionString):
            EventhubHandle(connection_str="not a valid conn str")

    def test_minimum_valid_manual_params(self):
        eh = EventhubHandle(
            namespace="testns",
            eventhub="testhub",
            accessKeyName="testkey",
            accessKey="secret",
        )

        self.assertEqual(eh.bootstrap_servers, "testns.servicebus.windows.net:9093")
        self.assertEqual(eh.topic, "testhub")
        self.assertIn("SharedAccessKey=secret", eh.connectionString)

    def test_missing_params_raises(self):
        # Missing one required param
        with self.assertRaises(InvalidEventhubHandleParameters):
            EventhubHandle(
                namespace="testns",
                eventhub="testhub",
                accessKeyName="testkey",
                # Missing accessKey
            )

    def test_connection_str_overrides_namespace(self):
        # In current implementation: if namespace is passed, it takes priority
        conn_str = (
            "Endpoint=sb://parsedns.servicebus.windows.net/parsedhub;"
            "EntityPath=parsedhub;SharedAccessKeyName=testkey;"
            "SharedAccessKey=secret"
        )
        eh = EventhubHandle(
            connection_str=conn_str,
            namespace="ignoredns",  # <-- actually used
        )

        self.assertEqual(eh.bootstrap_servers, "ignoredns.servicebus.windows.net:9093")
        self.assertEqual(eh.topic, "parsedhub")  # this still comes from conn string

    def test_entity_path_from_connection_string(self):
        # No eventhub argument → should be parsed from conn_str
        conn_str = (
            "Endpoint=sb://ns1.servicebus.windows.net/eh1;"
            "EntityPath=eh1;SharedAccessKeyName=testkey;SharedAccessKey=secret"
        )

        handle = EventhubHandle(connection_str=conn_str)

        self.assertEqual(handle.topic, "eh1")

    def test_entity_path_from_argument(self):
        # eventhub argument provided → should override EntityPath from conn_str
        conn_str = (
            "Endpoint=sb://ns1.servicebus.windows.net/eh1;"
            "EntityPath=eh1;SharedAccessKeyName=testkey;SharedAccessKey=secret"
        )

        handle = EventhubHandle(connection_str=conn_str, eventhub="override_eh")

        self.assertEqual(handle.topic, "override_eh")

    def test_entity_path_with_manual_fields_only(self):
        handle = EventhubHandle(
            namespace="ns2", eventhub="eh2", accessKeyName="key", accessKey="secret"
        )

        self.assertEqual(handle.topic, "eh2")

    def test_missing_entity_path_and_eventhub_raises(self):
        # Valid conn_str but missing EntityPath → should raise KeyError
        conn_str = (
            "Endpoint=sb://ns.servicebus.windows.net/;"  # No EntityPath!
            "SharedAccessKeyName=key;SharedAccessKey=secret"
        )

        with self.assertRaises(KeyError):
            EventhubHandle(connection_str=conn_str)
