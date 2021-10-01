from abc import abstractmethod

from pyspark.sql import DataFrame


class Transformer:
    def __init__(self):
        pass

    @abstractmethod
    def process(self, df: DataFrame) -> DataFrame:
        return df

    @abstractmethod
    def process_many(self, dataset: {}) -> DataFrame:
        pass


class DelegatingTransformer(Transformer):
    def __init__(self, inner_transformers: [Transformer]):
        super().__init__()
        self.inner_transformers = inner_transformers

    def get_transformers(self) -> [Transformer]:
        return self.inner_transformers

    def process(self, df: DataFrame) -> DataFrame:
        for transformer in self.inner_transformers:
            df = transformer.process(df)
        return df

    def process_many(self, dataset: {}) -> DataFrame:
        raise NotImplementedError
