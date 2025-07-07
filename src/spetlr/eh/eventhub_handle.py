import datetime
from typing import Dict
from urllib.parse import urlparse

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spetlr import Configurator
from spetlr.exceptions import (
    InvalidEventhubConnectionString,
    InvalidEventhubHandleParameters,
    InvalidEventhubWriteSchema,
)
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark
from spetlr.tables import TableHandle


class EventhubHandle(TableHandle):
    """
    Kafka-compatible Event Hub handle for reading and writing data in Apache Spark.

    This class provides functionality to interact with Azure Event Hubs through
    its Kafka-compatible endpoint using PySpark's Kafka source and sink. It allows
    structured and streaming reads, as well as structured writes, while supporting
    optional schema parsing of the message payload.

    Attributes:
        kafkaConfigs (dict): Kafka consumer/producer configuration.
        _schema (StructType): Optional schema used to parse JSON-encoded values.
        bootstrap_servers (str): Kafka-compatible Event Hub bootstrap server URL.
        topic (str): Event Hub name used as the Kafka topic.

    Methods:
        from_tc(id):
           Create an instance from configuration using the Configurator.
        read():
           Perform a batch read from the Event Hub.
        read_stream():
           Perform a streaming read from the Event Hub.
        write_or_append(df, mode, mergeSchema, overwriteSchema):
           Write data to the Event Hub.
        overwrite(df, mergeSchema, overwriteSchema):
           Alias for write_or_append in overwrite mode.
        append(df, mergeSchema):
           Alias for write_or_append in append mode.
        set_options_dict(options):
           Set the Kafka options dictionary.
        get_options_dict():
           Retrieve the Kafka options dictionary.
        get_schema():
           Retrieve the configured schema.
        set_schema(schema):
           Set a new schema for parsing incoming data.
    """

    def __init__(
        self,
        namespace: str = None,
        eventhub: str = None,
        consumer_group: str = None,
        connection_str: str = None,
        accessKeyName: str = None,
        accessKey: str = None,
        maxEventsPerTrigger: int = None,
        kafkaConfigs: dict = None,
        schema: StructType = None,
    ):

        if connection_str is None:
            if (
                namespace is None
                or eventhub is None
                or accessKeyName is None
                or accessKey is None
            ):
                raise InvalidEventhubHandleParameters(
                    "Either connectionString or "
                    "(namespace, eventhub, accessKeyName and accessKey) "
                    "have to be supplied"
                )

            self.connectionString = (
                f"Endpoint=sb://{namespace}.servicebus.windows.net/{eventhub};"
                f"EntityPath={eventhub};SharedAccessKeyName={accessKeyName};"
                f"SharedAccessKey={accessKey}"
            )
        else:
            self.connectionString = connection_str

            # Split up connection string to retrieve relevant info

            try:
                cstring_parts = {
                    p.split("=")[0]: p.split("=")[1]
                    for p in self.connectionString.split(";")
                }
            except IndexError:
                raise InvalidEventhubConnectionString(
                    "Invalid eventhub connection string!"
                )

        # The logic trust the explicit setted namespace
        # over the connectionstring
        if namespace is None:

            self.bootstrap_servers = (
                urlparse(cstring_parts["Endpoint"]).netloc + ":9093"
            )
        else:
            self.bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"

        self.topic = eventhub or cstring_parts["EntityPath"]

        self._schema = schema

        self.kafkaConfigs = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.topic,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            # ==== sasl.jass.config start ====
            "kafka.sasl.jaas.config": f"kafkashaded.org.apache.kafka"
            f".common.security.plain"
            f".PlainLoginModule required "
            f'username="$ConnectionString" '
            f'password="{self.connectionString}";',
            # ==== sasl.jass.config end ====
        }

        if maxEventsPerTrigger is not None:
            self.kafkaConfigs["maxOffsetsPerTrigger"] = str(maxEventsPerTrigger)

        if kafkaConfigs:
            self.kafkaConfigs.update(kafkaConfigs)

        if consumer_group:
            self.kafkaConfigs["kafka.group.id"] = consumer_group

    @classmethod
    def from_tc(
        cls,
        id: str,
        connection_str: str = None,
        accessKey: str = None,
    ) -> "EventhubHandle":
        tc = Configurator()

        _raw_maxEventsPerTrigger = tc.get(id, "eh_maxEventsPerTrigger", None)
        maxEventsPerTrigger = (
            int(_raw_maxEventsPerTrigger)
            if _raw_maxEventsPerTrigger is not None
            else None
        )

        return cls(
            namespace=tc.get(id, "eh_namespace", None),
            eventhub=tc.get(id, "eh_eventhub", None),
            consumer_group=tc.get(id, "eh_consumer_group", None),
            connection_str=connection_str or tc.get(id, "eh_connection_str", None),
            accessKeyName=tc.get(id, "eh_accessKeyName", None),
            accessKey=accessKey
            or tc.get(id, "eh_accessKey", None),  # Not recommended to set in .yml
            maxEventsPerTrigger=maxEventsPerTrigger,
            kafkaConfigs=tc.get(id, "eh_eventhubConfigs", None),
            schema=SchemaManager().get_schema(id, None),
        )

    def read(self) -> DataFrame:
        df = (
            Spark.get()
            .read.format("kafka")
            .option("startingOffsets", "earliest")
            .options(**self.kafkaConfigs)
            .load()
        )

        df = self._convert_kafka_schema_to_eventhub_schema(df)

        if self._schema:
            df = df.withColumn("Body", f.from_json("Body", self._schema))
        else:
            print(
                "No schema was detected in the EventhubHandle. "
                "Body is formatted as string..."
            )
        return df

    def read_stream(self):
        df = (
            Spark.get()
            .readStream.format("kafka")
            .option("startingOffsets", "earliest")
            .options(**self.kafkaConfigs)
            .load()
        )

        df = self._convert_kafka_schema_to_eventhub_schema(df)

        if self._schema:
            df = df.withColumn("Body", f.from_json("Body", self._schema))
        else:
            print(
                "No schema was detected in the EventhubHandle. "
                "Body is formatted as string..."
            )
        return df

    def write_or_append(
        self,
        df: DataFrame,
        mode: str,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
    ) -> None:
        """
        It is up to the user of the EventhubHandle class
        to provide a schema, that matches the write semantics

        Link: https://spark.apache.org/docs/
                latest/structured-streaming-kafka-integration.html#writing-data-to-kafka

        """

        assert mode in {"append", "overwrite"}

        if mode == "overwrite":
            print("Kafka does not support overwrite mode. Switching to append.")
            mode = "append"

        self._check_write_dataframe(df)

        writer = (
            df.write.format("kafka")
            .options(**self.kafkaConfigs)
            .mode(mode)
            .option("topic", self.topic)
        )
        return writer.save()

    def overwrite(
        self, df: DataFrame, mergeSchema: bool = None, overwriteSchema: bool = None
    ) -> None:
        return self.write_or_append(df, "overwrite")

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append")

    def set_options_dict(self, options: Dict[str, str]):
        self.kafkaConfigs = options

    def get_options_dict(self) -> Dict[str, str]:
        return self.kafkaConfigs

    def get_schema(self) -> StructType:
        return self._schema

    def set_schema(self, schema: StructType) -> None:
        self._schema = schema

    @staticmethod
    def _check_write_dataframe(df: DataFrame) -> None:
        _lower_cols = [col.lower() for col in df.columns]

        if "value" not in _lower_cols:
            raise InvalidEventhubWriteSchema("You must provide at least a Kafka value!")

    @staticmethod
    def _convert_kafka_schema_to_eventhub_schema(df: DataFrame) -> DataFrame:

        # Generate Unique id for the eventhub rows
        df = df.withColumn(
            "EventhubRowId",
            f.xxhash64(
                f.sha2(f.col("value").cast("string"), 256),
                f.sha2(f.col("timestamp").cast("string"), 256),
            ).cast("long"),
        )

        # Generate id for the eventhub rows using hashed body
        # Can be used for identify rows with same body
        df = df.withColumn(
            "BodyId",
            f.xxhash64(f.lit("0"), f.col("value").cast("string")).cast("long"),
        )

        # Add streaming time
        streaming_time = datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        )

        return df.select(
            f.col("EventhubRowId").cast("long").alias("EventhubRowId"),
            f.col("BodyId").cast("long").alias("BodyId"),
            f.col("offset").cast("bigint").alias("SequenceNumber"),
            f.col("partition").cast("int").alias("PartitionNumber"),
            f.col("timestamp").cast("DATE").alias("EnqueuedDate"),
            f.col("timestamp").cast("TIMESTAMP").alias("EnqueuedTime"),
            f.lit(streaming_time).alias("StreamingTime"),
            f.lit("{}").alias("Properties"),
            f.lit("{}").alias("SystemProperties"),
            f.col("value").cast("string").alias("Body"),
        )
