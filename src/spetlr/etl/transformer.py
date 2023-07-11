import warnings
from typing import List, Union

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Transformer(EtlBase):
    """
    This the base Transformer class for the OELT framework.

    If you only want to transform a single input dataframe,
    implement `process`
    If you want to transform a set of dataframes,
    implement `process_many`

    In regards to the etl step, the behavior of the transformation is controlled
    by the parameters dataset_input_keys, dataset_output_key and consume_inputs.

    Attributes:
    ----------
        dataset_input_keys : Union[str, List[str]]
            str or list of input dataset keys that will be used for
            either process or process_many
        dataset_output_key : str
            name of the output key that we be added to the dataset after transformation
        consume_inputs : bool
            Flag to control dataset consuming behavior
    """

    def __init__(
        self,
        dataset_key: str = None,
        *,
        dataset_input_keys: Union[str, List[str]] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        if dataset_key is not None:
            warnings.warn(
                "The parameter dataset_key is deprecated, use dataset_output_key instead."  # noqa: E501
            )
            dataset_output_key = dataset_key

        if dataset_input_keys is None:
            self.dataset_input_key_list = []
        elif isinstance(dataset_input_keys, str):
            self.dataset_input_key_list = [dataset_input_keys]
        else:
            self.dataset_input_key_list = dataset_input_keys

        if dataset_output_key is None:
            self.dataset_output_key = type(self).__name__
        else:
            self.dataset_output_key = dataset_output_key

        self.consume_inputs = consume_inputs

    def etl(self, inputs: dataset_group) -> dataset_group:
        # If dataset_input_key_list is empty, it will be set to all keys in inputs
        if len(self.dataset_input_key_list) == 0:
            dataset_input_key_list = list(inputs.keys())
        else:
            dataset_input_key_list = self.dataset_input_key_list

        # Call process or process_many depending on the input_key count
        if len(dataset_input_key_list) == 1:
            df = self.process(inputs[dataset_input_key_list[0]])
        else:
            datasetFilteret = {
                datasetKey: df
                for datasetKey, df in inputs.items()
                if datasetKey in dataset_input_key_list
            }
            df = self.process_many(datasetFilteret)

        if self.consume_inputs:
            for key in dataset_input_key_list:
                inputs.pop(key)

        inputs[self.dataset_output_key] = df
        return inputs

    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
