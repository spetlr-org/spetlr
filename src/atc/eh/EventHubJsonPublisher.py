from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from atc.eh import EventHubStream
from atc.etl import Loader


class EventHubJsonPublisher(Loader):
    """Save the rows of the incoming dataframe to the eventhub stream,
    formatted as json documents."""

    def __init__(self, eh: EventHubStream):
        super().__init__()
        self.eh = eh

    def save(self, df: DataFrame) -> None:
        self.eh.save_data(
            df.select(
                f.encode(
                    f.to_json(f.struct("*")),
                    "utf-8",
                ).alias("body")
            )
        )
