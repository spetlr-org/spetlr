from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Loader(EtlBase):
    """
    A loader is a data sink, which is indicated by the fact that
    the save method has no return.

    In regards to the etl step, a loader USES the input dataset(s)
    and does not consume or change it.
    """

    def __init__(self):
        super().__init__()

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(inputs) == 1:
            df = next(iter(inputs.values()))
            self.save(df)
            return inputs
        self.save_many(inputs)
        return inputs

    def save(self, df: DataFrame) -> None:
        raise NotImplementedError()

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()
