from atc.etl import Transformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class MessageTypeFilterTransformer(Transformer):
    def __init__(self, value: str, message_type: str = "messageType"):
        """ """
        super().__init__()
        self.value = value
        self.message_type = message_type

    def process(self, df: DataFrame) -> DataFrame:
        return df.filter(F.col(self.message_type) == self.value)
