from typing import List, Union

from pyspark.sql import DataFrame

from .types import MLModelBase, pipeline_stage_type


class Fitter(MLModelBase):
    """ """

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

    def pipe(self, input: DataFrame, input_pipe: Pipeline) -> Pipeline:
        stage = self.apply_model(input)
        return input_pipe.add(stage)

    @abstractmethod
    def apply_model(self, input: DataFrame) -> pipeline_stage_type:
        raise NotImplementedError()
