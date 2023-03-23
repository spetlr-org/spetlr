"""
This file sets up the EventHub that is deployed
as part of the spetlr integration pipeline
"""
from spetlr.eh import EventHubStream
from spetlr.functions import init_dbutils


class SpetlrEh(EventHubStream):
    def __init__(self):
        super().__init__(
            connection_str=init_dbutils().secrets.get("secrets", "EventHubConnection"),
            entity_path="spetlreh",
            consumer_group="$Default",
        )
