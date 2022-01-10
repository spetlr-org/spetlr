from abc import abstractmethod

from pyspark.sql import DataFrame

from .types import dataset_group, EtlBase


class Loader(EtlBase):
    """
    A loader is a data sink, which is indicated by the fact that
    the save method has no return.

    In regards to the etl step, a loader USES the input dataset(s)
    and does not consume or change it.
    """

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(inputs) == 1:
            df = next(iter(inputs.values()))
            self.save(df)
            return inputs
        self.save_many(inputs)
        return inputs

    @abstractmethod
    def save(self, df: DataFrame) -> None:
        pass

    @abstractmethod
    def save_many(self, datasets: dataset_group) -> None:
        pass
