from abc import abstractmethod
from typing import List

from pyspark.sql import DataFrame


class Loader:
    @abstractmethod
    def save(self, df: DataFrame) -> DataFrame:
        return df


class DelegatingLoader(Loader):

    def __init__(self, inner_loaders: List[Loader]):
        super().__init__()
        self.inner_loaders = inner_loaders

    def get_loaders(self) -> List[Loader]:
        return self.inner_loaders

    def save(self, df: DataFrame) -> DataFrame:
        for loader in self.inner_loaders:
            loader.save(df)
        return df
