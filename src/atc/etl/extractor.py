from abc import abstractmethod

from pyspark.sql import DataFrame

from .types import dataset_group, EtlBase


class Extractor(EtlBase):
    """In some cases extractors depend on other extractors,
    for these situations, the datasets from previous steps
    are accessible in the variable self.previous_extractions.

    In regards to the etl step, the extractor ADDs its dataset
    to the total set of datasets.
    """

    def __init__(self, dataset_key: str = None):
        if dataset_key is None:
            dataset_key = type(self).__name__
        self.dataset_key = dataset_key
        self.previous_extractions: dataset_group = {}

    def etl(self, inputs: dataset_group) -> dataset_group:
        self.previous_extractions = inputs
        new_df = self.read()
        inputs[self.dataset_key] = new_df
        return inputs

    @abstractmethod
    def read(self) -> DataFrame:
        pass
