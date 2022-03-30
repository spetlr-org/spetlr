# Defines a class for structured streaming from and to Azure Event Hubs

from pyspark.sql import DataFrame

from atc.spark import Spark


class EventHubStream:
    def __init__(
        self,
        connection_str: str,
        entity_path: str,
        consumer_group: str,
        max_events_per_trigger: int = 1_000_000,
    ):
        """
        Initializes an Azure EventHub.
        :param connection_str: Connection string to the Event Hubs Namespace
            (not the actual Event Hub!)
        :param entity_path: Name of the Event Hub within the Event Hubs Namespace
        :param consumer_group: Name of the Event Hub consumer group
        :param max_events_per_trigger: Maximum number of events to be processed
            in one go.
        Warning: The number must be at least twice as big as the average number of
        new events created during the time
        it takes to process and save the existing events, otherwise
        the save_stream_batch() method will never finish.
        The number must not be too high, though, to not to exceed memory resources.
        """

        unencrypted = "{};EntityPath={}".format(connection_str, entity_path)
        # noinspection PyProtectedMember
        encrypted = Spark.get()._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            unencrypted
        )
        self.max_events_per_trigger = max_events_per_trigger
        self.config = {
            "eventhubs.consumerGroup": consumer_group,
            "eventhubs.connectionString": encrypted,
            "maxEventsPerTrigger": max_events_per_trigger,
        }

    def save_data(self, df_source: DataFrame):
        (df_source.write.format("eventhubs").options(**self.config).save())
