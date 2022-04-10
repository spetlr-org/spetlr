"""
This file sets up the EventHub that is deployed as part of the atc integration pipeline
"""
from atc.eh import EventHubStream
from atc.functions import init_dbutils


class AtcEh(EventHubStream):
    def __init__(self):
        super().__init__(
            connection_str=init_dbutils().secrets.get("atc", "EventHubConnection"),
            entity_path="atceh",
            consumer_group="$Default",
        )
