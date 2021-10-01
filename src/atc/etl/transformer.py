from abc import abstractmethod

from pyspark.sql import DataFrame


class Transformer:
    def __init__(self):
        pass

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        pass


class DelegatingTransformer(Transformer):
    def __init__(self, inner_transformers: [Transformer]):
        super().__init__()
        self.inner_transformers = inner_transformers

    @abstractmethod
    def process(self, dataset: {}) -> DataFrame:
        pass
