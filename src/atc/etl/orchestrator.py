from abc import abstractmethod
from typing import List, Union

from pyspark.sql.dataframe import DataFrame

from .extractor import Extractor, DelegatingExtractor
from .loader import Loader, DelegatingLoader
from .transformer import Transformer, DelegatingTransformer, MultiInputTransformer


class Orchestration:
    @staticmethod
    def extract_from(extractor: Extractor):
        return OrchestratorBuilder(extractor)


class OrchestratorBuilderException(Exception):
    pass


class LogicError(OrchestratorBuilderException):
    pass


class Orchestrator:
    extractor: Union[Extractor, DelegatingExtractor]
    transformer: Union[Transformer, DelegatingTransformer]
    loader: Union[Loader, DelegatingLoader]

    @abstractmethod
    def execute(self) -> DataFrame:
        pass


class SingleOrchestrator(Orchestrator):
    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class NoTransformOrchestrator(Orchestrator):
    def __init__(self, extractor: Extractor, loader: Loader):
        self.loader = loader
        self.extractor = extractor

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        return self.loader.save(df)


class MultipleExtractOrchestrator(Orchestrator):
    def __init__(
        self,
        extractor: DelegatingExtractor,
        transformer: MultiInputTransformer,
        loader: Loader,
    ):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self) -> DataFrame:
        dataset = self.extractor.read()
        df = self.transformer.process_many(dataset)
        return self.loader.save(df)


class MultipleTransformOrchestrator(Orchestrator):
    def __init__(
        self, extractor: Extractor, transformer: DelegatingTransformer, loader: Loader
    ):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class MultipleLoaderOrchestrator(Orchestrator):
    def __init__(
        self, extractor: Extractor, transformer: Transformer, loader: DelegatingLoader
    ):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self) -> DataFrame:
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class OrchestratorBuilder(Orchestrator):
    extractors: List[Extractor]
    transformers: List[Union[MultiInputTransformer, Transformer]]
    loaders: List[Loader]

    def __init__(self, extractor: Extractor):
        self.extractors = [extractor]
        self.transformers = []
        self.loaders = []

    def extract_from(self, extractor: Extractor) -> "OrchestratorBuilder":
        if self.transformers or self.loaders:
            raise OrchestratorBuilderException(
                "Set all extractors first"
            )
        self.extractors.append(extractor)
        return self

    def transform_with(self, transformer: Transformer) -> "OrchestratorBuilder":
        if not self.extractors:
            raise OrchestratorBuilderException(
                "Set all extractors before the transformers"
            )
        if self.loaders:
            raise OrchestratorBuilderException(
                "Cannot add transformer after loader"
            )
        if len(self.extractors) > 1:
            if not isinstance(transformer, MultiInputTransformer):
                raise OrchestratorBuilderException(
                    "Multiple extractors require a MultiInputTransformer"
                )
            if len(self.transformers):
                raise OrchestratorBuilderException(
                    "Multiple transformers for multiple spurces not currently supported."
                )
        self.transformers.append(transformer)
        return self

    def load_into(self, loader: Loader) -> "OrchestratorBuilder":
        if not self.extractors:
            raise OrchestratorBuilderException("You need to set extractor(s) first.")
        self.loaders.append(loader)
        return self

    def execute(self) -> DataFrame:
        return self.build().execute()

    def build(self) -> Orchestrator:
        le = len(self.extractors)
        lt = len(self.transformers)
        ll = len(self.loaders)
        if le == lt == ll == 1:
            return SingleOrchestrator(self.extractors[0], self.transformers[0], self.loaders[0])
        elif le > 1 and lt == ll == 1:
            return MultipleExtractOrchestrator(
                DelegatingExtractor(self.extractors), self.transformers[0], self.loaders[0]
            )
        elif lt > 1 and le == ll == 1:
            return MultipleTransformOrchestrator(
                self.extractors[0], DelegatingTransformer(self.transformers), self.loaders[0]
            )
        elif lt == 0 and le == ll == 1:
            return NoTransformOrchestrator(self.extractors[0], self.loaders[0])
        elif le==lt==1 and ll>1:
            return MultipleLoaderOrchestrator(self.extractors[0], self.transformers[0],DelegatingLoader(self.loaders))
        else:
            raise LogicError(
                f"No supported orchestrator for "
                f"{le} extractors, "
                f"{le} transformers "
                f"and {ll} loaders"
            )
