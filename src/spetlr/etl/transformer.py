import warnings
from typing import List

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Transformer(EtlBase):
    """
    This is the base Transformer class for the OETL framework.

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
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        if dataset_key is not None:
            warnings.warn(
                "The parameter dataset_key is deprecated, use dataset_output_key instead."  # noqa: E501
            )
            dataset_output_key = dataset_key

        self.dataset_input_keys = dataset_input_keys

        if dataset_output_key is None:
            self.dataset_output_key = type(self).__name__
        else:
            self.dataset_output_key = dataset_output_key

        self.consume_inputs = consume_inputs

    def etl(self, inputs: dataset_group) -> dataset_group:
        # If dataset_input_keys is None, it will be set to all keys in inputs
        dataset_input_keys = self.dataset_input_keys or list(inputs.keys())

        # Call process or process_many depending on the input_key count
        if len(dataset_input_keys) == 1:
            df = self.process(inputs[dataset_input_keys[0]])
        else:
            inputs_filtered = {
                datasetKey: df
                for datasetKey, df in inputs.items()
                if datasetKey in dataset_input_keys
            }
            df = self.process_many(inputs_filtered)

        if self.consume_inputs:
            for key in dataset_input_keys:
                inputs.pop(key)

        # dataset_output_key is added to inputs after pop is called
        # This ensures that dataset_output_key is not popped.
        inputs[self.dataset_output_key] = df
        return inputs

    def process(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError()

    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
