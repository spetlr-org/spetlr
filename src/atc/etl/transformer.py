from abc import abstractmethod

from pyspark.sql import DataFrame

from atc.etl.types import dataset_group, EtlBase


class Transformer(EtlBase):
    """If you only want to transform a single input dataframe,
    implement `process`
    if you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the transformer CONSUMES the inputs
    and ADDs the result of its transformation stage.
    """

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(inputs) == 1:
            for v in inputs.values():
                return {type(self).__name__: self.process(v)}
        return {type(self).__name__: self.process_many(inputs)}

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def process_many(self, datasets: dataset_group) -> DataFrame:
        pass
