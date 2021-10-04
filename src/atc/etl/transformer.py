from abc import abstractmethod
from typing import List, Dict

from pyspark.sql import DataFrame


class Transformer:
    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        return df


class MultiInputTransformer:
    @abstractmethod
    def process_many(self, dataset: Dict[str, DataFrame]) -> DataFrame:
        pass


class DelegatingTransformer(Transformer):
    def __init__(self, inner_transformers: List[Transformer]):
        super().__init__()
        self.inner_transformers = inner_transformers

    def get_transformers(self) -> List[Transformer]:
        return self.inner_transformers

    def process(self, df: DataFrame) -> DataFrame:
        for transformer in self.inner_transformers:
            df = transformer.process(df)
        return df
