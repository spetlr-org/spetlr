from abc import abstractmethod
from typing import Dict

from pyspark.sql import DataFrame

dataset_group = Dict[str, DataFrame]


class EtlBase:
    @abstractmethod
    def etl(self, inputs: dataset_group) -> dataset_group:
        pass
