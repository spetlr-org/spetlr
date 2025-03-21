from typing import Dict

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType

from spetlr import Configurator
from spetlr.exceptions import IncorrectSchemaException
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark
from spetlr.tables import TableHandle


class EventhubHandle(TableHandle):
    def __init__(
        self,
        consumer_group: str,
        namespace: str,
        eventhub: str,
        accessKeyName: str,
        accessKey: str,
        maxEventsPerTrigger: int = None,
        kafkaConfigs: dict = None,
        schema: StructType = None,
    ):
        self._schema = schema
        self.bootstrap_servers = f"{namespace}.servicebus.windows.net:9093"
        self.topic = eventhub

        self.kafkaConfigs = {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.topic,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            # ==== sasl.jass.config start ====
            "kafka.sasl.jaas.config": f"org.apache.kafka"
            f".common.security.plain"
            f".PlainLoginModule required "
            f'username="$ConnectionString" '
            f'password="Endpoint=sb://{namespace}'
            f".servicebus.windows.net/;"
            f"SharedAccessKeyName={accessKeyName};"
            f'SharedAccessKey={accessKey}";',
            # ==== sasl.jass.config end ====
            "maxOffsetsPerTrigger": str(maxEventsPerTrigger),
        }

        if kafkaConfigs:
            self.kafkaConfigs.update(kafkaConfigs)

        if consumer_group:
            self.kafkaConfigs["kafka.group.id"] = consumer_group

    from datetime import datetime

    @classmethod
    def from_tc(cls, id: str) -> "EventhubHandle":
        tc = Configurator()

        namespace = tc.get(id, "eh_namespace", None)
        eventhub = tc.get(id, "eh_eventhub", None)
        accessKeyName = tc.get(id, "eh_accessKeyName", None)
        accessKey = tc.get(id, "eh_accessKey", None)
        consumer_group = tc.get(id, "eh_consumer_group", None)
        maxEventsPerTrigger = int(tc.get(id, "eh_maxEventsPerTrigger", None))
        _start_time = tc.get(id, "eh_startEnqueuedTime", None)
        schema_id = tc.get(id, "schema", None)

        schema = SchemaManager().get_schema(schema_id, None)

        return cls(
            consumer_group=consumer_group,
            namespace=namespace,
            eventhub=eventhub,
            accessKeyName=accessKeyName,
            accessKey=accessKey,
            maxEventsPerTrigger=maxEventsPerTrigger,
            schema=schema,
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

        writer = df.write.format("kafka").options(**self.kafkaConfigs).mode(mode)
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
