import json
from datetime import datetime
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
    def __init__(
        self,
        consumer_group: str,
        connection_str: str = None,
        namespace: str = None,
        eventhub: str = None,
        accessKeyName: str = None,
        accessKey: str = None,
        maxEventsPerTrigger: int = 10000,
        startEnqueuedTime: datetime = None,
        eventhubConfigs: dict = None,
        schema: StructType = None,
        encrypt: bool = True,
    ):
        self._schema = schema

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

        if eventhubConfigs is None:
            self.set_options_dict({})
        else:
            self.set_options_dict(eventhubConfigs)

        if consumer_group:
            self.eventhubConfigs["eventhubs.consumerGroup"] = consumer_group

        if encrypt:
            self.connectionString = Spark.get().sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(  # noqa: E501
                self.connectionString
            )

        self.eventhubConfigs["eventhubs.connectionString"] = self.connectionString

        self.eventhubConfigs["maxEventsPerTrigger"] = str(maxEventsPerTrigger)

        self.startingEventPosition = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True,
        }

        if startEnqueuedTime:
            self.startingEventPosition["enqueuedTime"] = startEnqueuedTime.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )  # Start from timestamp
        else:
            # Default is to start from beginning of stream
            self.startingEventPosition["offset"] = "-1"  # Start stream from beginning

        self.eventhubConfigs["eventhubs.startingPosition"] = json.dumps(
            self.startingEventPosition
        )

    @classmethod
    def from_tc(
        cls,
        id: str,
        connection_str: str = None,
        accessKey: str = None,
    ) -> "EventhubHandle":
        tc = Configurator()

        _start_time = tc.get(id, "eh_startEnqueuedTime", None)

        if _start_time:
            _start_time = datetime.strptime(_start_time, "%Y-%m-%dT%H:%M:%S.%fZ")

        return cls(
            consumer_group=tc.get(id, "eh_consumer_group", None),
            connection_str=connection_str or tc.get(id, "eh_connection_str", None),
            namespace=tc.get(id, "eh_namespace", None),
            eventhub=tc.get(id, "eh_eventhub", None),
            accessKeyName=tc.get(id, "eh_accessKeyName", None),
            accessKey=accessKey
            or tc.get(id, "eh_accessKey", None),  # Not recommended to set in .yml
            maxEventsPerTrigger=int(
                tc.get(id, "eh_maxEventsPerTrigger", str(1_000_000))
            ),
            startEnqueuedTime=_start_time,
            eventhubConfigs=tc.get(id, "eh_eventhubConfigs", None),
            schema=SchemaManager().get_schema(id, None),
            encrypt=tc.get(id, "eh_encrypt", True),
        )

    def _read_with_schema(self, df: DataFrame):
        if self._schema:
            return df.withColumn(
                "body", f.from_json(f.col("body").cast(StringType()), self._schema)
            )
        else:
            print(
                "No schema was detected in the EventhubHandle. "
                "Body is formatted as string..."
            )
            return df.withColumn("body", f.col("body").cast(StringType()))

    def read(self) -> DataFrame:

        df = Spark.get().read.format("eventhubs").options(**self.eventhubConfigs).load()
        return self._read_with_schema(df)

    def read_stream(self):

        df = (
            Spark.get()
            .readStream.format("eventhubs")
            .options(**self.eventhubConfigs)
            .load()
        )

        return self._read_with_schema(df)

    def write_or_append(
        self,
        df: DataFrame,
        mode: str,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
    ) -> None:
        assert mode in {"append", "overwrite"}

        if mode == "overwrite":
            print(
                """
                 Loading to an event hub has no 'overwrite' mode
                 Loading mode is changed to 'append'
                 Consider changing the load mode to 'append' in your configuration
                """
            )

        mode = "append"

        if "body" in df.columns:
            raise IncorrectSchemaException(
                """
                The input df should only have a single column named 'body'
                Note that data in the 'body' column should be formatted to json
                """
            )

        df = self._create_write_dataframe(df)

        writer = df.write.format("eventhubs").options(**self.eventhubConfigs).mode(mode)
        if mergeSchema is not None:
            writer = writer.option("mergeSchema", "true" if mergeSchema else "false")

        if overwriteSchema is not None:
            writer = writer.option(
                "overwriteSchema", "true" if overwriteSchema else "false"
            )

        return writer.save()

    def overwrite(
        self, df: DataFrame, mergeSchema: bool = None, overwriteSchema: bool = None
    ) -> None:
        return self.write_or_append(
            df, "overwrite", mergeSchema=mergeSchema, overwriteSchema=overwriteSchema
        )

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        return self.write_or_append(df, "append", mergeSchema=mergeSchema)

    def set_options_dict(self, options: Dict[str, str]):
        self.eventhubConfigs = options

    def get_options_dict(self) -> Dict[str, str]:
        return self.eventhubConfigs

    def get_schema(self) -> StructType:
        return self._schema

    def set_schema(self, schema: StructType) -> None:
        self._schema = schema

    @staticmethod
    def _create_write_dataframe(df: DataFrame) -> DataFrame:

        return (
            df.withColumn("body", f.struct(df.columns))
            .withColumn("body", f.to_json("body"))
            .select("body")
        )
