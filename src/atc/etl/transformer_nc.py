from abc import abstractmethod
from typing import List, Union

from pyspark.sql import DataFrame

from atc.etl.types import EtlBase, dataset_group


class TransformerNC(EtlBase):
    """If you only want to transform a single input dataframe,
    implement `process`
    If you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the TransformerNC does NOT CONSUME the inputs
    and ADDs the result of its transformation stage to the dataset dict.
    """

    def __init__(
        self,
        *,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
    ):
        if dataset_output_key is not None:
            self.dataset_output_key = dataset_output_key
        else:
            self.dataset_output_key = type(self).__name__

        if dataset_input_keys is None:
            self.dataset_input_key_list = []
        elif isinstance(dataset_input_keys, str):
            self.dataset_input_key_list = [dataset_input_keys]
        else:
            self.dataset_input_key_list = dataset_input_keys

    def etl(self, inputs: dataset_group) -> dataset_group:
        if len(self.dataset_input_key_list) > 0:
            if len(self.dataset_input_key_list) == 1:
                df = self.process(inputs[self.dataset_input_key_list[0]])
            else:
                datasetFilteret = {
                    datasetKey: df
                    for datasetKey, df in inputs.items()
                    if datasetKey in self.dataset_input_key_list
                }
                df = self.process_many(datasetFilteret)
        elif len(inputs) == 1:
            df = self.process(next(iter(inputs.values())))
        else:
            df = self.process_many(inputs)

        inputs[self.dataset_output_key] = df
        return inputs

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
