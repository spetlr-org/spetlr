from abc import abstractmethod

from pyspark.sql import DataFrame


class Extractor:
    def __init__(self):
        pass

    @abstractmethod
    def read(self) -> DataFrame:
        pass


class DelegatingExtractor(Extractor):
    def __init__(self, inner_extractors: [Extractor]):
        super().__init__()
        self.inner_extractors = inner_extractors

    def get_extractors(self) -> [Extractor]:
        return self.inner_extractors

    def read(self) -> dict:
        dataset = {}
        for extractor in self.inner_extractors:
            dataset[type(extractor).__name__] = extractor.read()
        return dataset
