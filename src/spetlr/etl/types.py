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
