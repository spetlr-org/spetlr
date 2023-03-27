from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Transformer(EtlBase):
    """If you only want to transform a single input dataframe,
    implement `process`
    if you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the transformer CONSUMES the inputs
    and ADDs the result of its transformation stage.
    """

    def __init__(self, dataset_key: str = None):
        super().__init__()
        if dataset_key is None:
            dataset_key = type(self).__name__
        self.dataset_key = dataset_key

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(inputs) == 1:
            df = next(iter(inputs.values()))
            return {self.dataset_key: self.process(df)}
        return {self.dataset_key: self.process_many(inputs)}

    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
