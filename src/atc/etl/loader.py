from abc import abstractmethod

from pyspark.sql import DataFrame


class Loader:
    def __init__(self):
        pass

    @abstractmethod
    def save(self, df: DataFrame) -> DataFrame:
        pass
