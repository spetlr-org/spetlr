from atc.etl.extractor import Extractor
from atc.spark import Spark

import json
from pyspark.sql import DataFrame

class EventhubStreamExtractor(Extractor):
    def __init__(self, consumerGroup: str, connectionString: str = None, namespace: str = None, eventhub: str = None, accessKeyName: str = None, accessKey: str = None, maxEventsPerTrigger: int = 10000):
        self.spark = Spark.get()

        # If connectionString is missing, create it from namespace, eventhub accessKeyName and accessKey
        if connectionString is None:
            connectionString = f"Endpoint=sb://{namespace}.servicebus.windows.net/{eventhub};EntityPath={eventhub};SharedAccessKeyName={accessKeyName};SharedAccessKey={accessKey}"

        # Define where to start eventhub stream
        # It can be done from offset, seqence number or timestamp
        # Below setting will start stream from the beginning
        startingEventPosition = {
            "offset": "-1", #Start stream from beginning
            "seqNo": -1, #not in use
            "enqueuedTime": None, #not in use
            "isInclusive": True
        }

        self.config = {
            "eventhubs.connectionString": self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
            "maxEventsPerTrigger": maxEventsPerTrigger,
            "eventhubs.consumerGroup": consumerGroup,
            "eventhubs.startingPosition": json.dumps(startingEventPosition)
        }

    def read(self) -> DataFrame:
        print(f"Read eventhub data stream")

        df = self.spark.readStream \
            .format("eventhubs") \
            .options(**self.config) \
            .load()
        
        return df