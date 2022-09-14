from typing import List

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class ExtendedLoader(EtlBase):
    """
    An ExtendedLoader is a data sink, which is indicated by the fact that
    the save method has no return.

    In regards to the etl step, an ExtendedLoader USES the input dataset(s)
    and does not consume or change it.
    """

    def __init__(
        self, dataset_input_key: str = None, dataset_input_key_list: List[str] = None
    ):
        self.dataset_input_key = dataset_input_key
        self.dataset_input_key_list = dataset_input_key_list

    def etl(self, inputs: dataset_group) -> dataset_group:

        if self.dataset_input_key:
            self.save(inputs[self.dataset_input_key])
        elif self.dataset_input_key_list:
            self.save_many(
                {
                    datasetKey: df
                    for datasetKey, df in inputs.items()
                    if datasetKey in self.dataset_input_key_list
                }
            )
        elif len(inputs) == 1:
            self.save(next(iter(inputs.values())))
        else:
            self.save_many(inputs)

        return inputs

    def save(self, df: DataFrame) -> None:
        raise NotImplementedError()

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()
