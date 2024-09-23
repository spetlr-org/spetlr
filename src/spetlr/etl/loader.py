from typing import List, Union

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Loader(EtlBase):
    """
    A loader is a data sink, which is indicated by the fact that
    the save method has no return.

    In regards to the etl step, a loader USES the input dataset(s)
    and does not consume or change it.
    """

    def __init__(
        self,
        *,
        dataset_input_keys: Union[str, List[str]] = None,
    ):
        if dataset_input_keys is None:
            self.dataset_input_key_list = []
        elif isinstance(dataset_input_keys, str):
            self.dataset_input_key_list = [dataset_input_keys]
        else:
            self.dataset_input_key_list = dataset_input_keys

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(self.dataset_input_key_list) > 0:
            if len(self.dataset_input_key_list) == 1:
                self.save(inputs[self.dataset_input_key_list[0]])
            else:
                datasetFilteret = {
                    datasetKey: df
                    for datasetKey, df in inputs.items()
                    if datasetKey in self.dataset_input_key_list
                }
                self.save_many(datasetFilteret)
        elif len(inputs) == 1:
            self.save(next(iter(inputs.values())))
        else:
            self.save_many(inputs)

        return inputs

    def save(self, df: DataFrame) -> None:
        raise NotImplementedError()

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()
