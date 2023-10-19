from abc import abstractmethod

from pyspark.sql import DataFrame

from spetlr.etl import EtlBase, dataset_group


class LazyExtractor(EtlBase):
    """In some cases extractors depend on other extractors,
    for these situations, the datasets from previous steps
    are accessible in the variable self.previous_extractions.

    In regards to the etl step, the extractor ADDs its dataset
    to the total set of datasets.

    This Lazy Extractor is very similar to the standard extractor
    with the key difference that it will only read its target
    if the dataset key is previously missing.
    """

    def __init__(self, dataset_key: str = None):
        super().__init__()
        self.dataset_key = dataset_key or type(self).__name__
        self.previous_extractions: dataset_group = {}

    def etl(self, inputs: dataset_group) -> dataset_group:
        self.previous_extractions = inputs
        if self.dataset_key not in inputs:
            inputs[self.dataset_key] = self.read()

        return inputs

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplementedError()
