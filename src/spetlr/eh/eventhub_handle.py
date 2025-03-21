from typing import Dict

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType

from spetlr import Configurator
from spetlr.exceptions import IncorrectSchemaException, InvalidEventhubHandleParameters
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
        namespace: str,
        eventhub: str,
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

        self._schema = schema
        self.bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
        self.topic = eventhub

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

    from datetime import datetime

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
        df = Spark.get().read.format("kafka").options(**self.kafkaConfigs).load()

        # Decode and parse value (not body anymore)
        df = df.withColumn("value", f.col("value").cast(StringType()))

        if self._schema:
            df = df.withColumn("value", f.from_json("value", self._schema))
        else:
            print(
                "No schema was detected in the EventhubHandle. "
                "Body is formatted as string..."
            )
        return df

    def read_stream(self):
        return (
            Spark.get().readStream.format("kafka").options(**self.kafkaConfigs).load()
        )

    def write_or_append(
        self,
        df: DataFrame,
        mode: str,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
    ) -> None:
        assert mode in {"append", "overwrite"}

        if mode == "overwrite":
            print("Kafka does not support overwrite mode. Switching to append.")
            mode = "append"

        if "value" in df.columns:
            raise IncorrectSchemaException(
                "Input DataFrame should only have one column named 'value'."
            )

        df = self._create_write_dataframe(df)

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
    def _create_write_dataframe(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("value", f.struct(df.columns))
            .withColumn("value", f.to_json("value"))
            .selectExpr("CAST(value AS STRING)")
        )
