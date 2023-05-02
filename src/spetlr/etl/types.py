from abc import abstractmethod
from typing import Dict

from pyspark.sql import DataFrame

dataset_group = Dict[str, DataFrame]


class EtlBase:
    def __init__(self):
        super().__init__()

    @abstractmethod
    def etl(self, inputs: dataset_group) -> dataset_group:
        raise NotImplementedError()


class MLBase:
    def __init__(self):
        super().__init__()

    @abstractmethod
    def pipe(self, input_pipe: Pipeline) -> Pipeline:
        raise NotImplementedError()


class MLModelBase:
    def __init__(self):
        super().__init__()

    @abstractmethod
    def pipe(self, input: DataFrame, input_pipe: Pipeline) -> Pipeline:
        raise NotImplementedError()
