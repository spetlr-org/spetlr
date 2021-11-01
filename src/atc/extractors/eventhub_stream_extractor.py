from atc.etl.extractor import Extractor
from atc.spark import Spark

import json
from pyspark.sql import DataFrame

class InvalidEventhubStreamExtractorParameters(Exception):
    pass

class EventhubStreamExtractor(Extractor):
    def __init__(
        self,
        consumerGroup: str,
        connectionString: str = None,
        namespace: str = None,
        eventhub: str = None,
        accessKeyName: str = None,
        accessKey: str = None,
        maxEventsPerTrigger: int = 10000
    ):
        """
        :param consumerGroup: the eventhub consumerGroup to use for streaming
        :param connectionString: connectionString to the eventhub, if not supplied namespace, eventhub, accessKeyName and accessKey have to be instead
        :param namespace: the eventhub namespace to use for streaming, can be ignored if connectionString is supplied
        :param eventhub: the eventhub name to use for streaming, can be ignored if connectionString is supplied
        :param accessKeyName: the eventhub accessKeyName to use for streaming, can be ignored if connectionString is supplied
        :param accessKey: the eventhub accessKey to use for streaming, can be ignored if connectionString is supplied
        :param maxEventsPerTrigger: the number of events handled per mico trigger in stream
        """

        if (connectionString is None and (namespace is None or eventhub is None or accessKeyName is None or accessKey is None)):
            raise InvalidEventhubStreamExtractorParameters("Either connectionString or (namespace, eventhub, accessKeyName and accessKey) have to be supplied")

        self.spark = Spark.get()
        self.consumerGroup = consumerGroup
        self.connectionString = connectionString
        self.namespace = namespace
        self.eventhub = eventhub
        self.accessKeyName = accessKeyName
        self.accessKey = accessKey
        self.maxEventsPerTrigger = maxEventsPerTrigger

        # If connectionString is missing, create it from namespace, eventhub, accessKeyName and accessKey
        if self.connectionString is None:
            self.connectionString = f"Endpoint=sb://{self.namespace}.servicebus.windows.net/{self.eventhub};EntityPath={self.eventhub};SharedAccessKeyName={self.accessKeyName};SharedAccessKey={self.accessKey}"

        # Define where to start eventhub stream
        # It can be done from offset, seqence number or timestamp
        # Below setting will start stream from the beginning
        self.startingEventPosition = {
            "offset": "-1",  # Start stream from beginning
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True,
        }

    def read(self) -> DataFrame:
        print(f"Read eventhub data stream")

        config = {
            "eventhubs.connectionString": self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.connectionString),
            "maxEventsPerTrigger": self.maxEventsPerTrigger,
            "eventhubs.consumerGroup": self.consumerGroup,
            "eventhubs.startingPosition": json.dumps(self.startingEventPosition),
        }

        df = self.spark.readStream.format("eventhubs").options(**config).load()

        return df
