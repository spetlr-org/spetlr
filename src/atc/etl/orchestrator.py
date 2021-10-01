from abc import abstractmethod

from .extractor import Extractor, DelegatingExtractor
from .loader import Loader
from .transformer import Transformer, DelegatingTransformer


class OrchestratorBase:

    @abstractmethod
    def execute(self):
        pass


class Orchestrator(OrchestratorBase):
    def __init__(self,
                 extractor: Extractor,
                 transformer: Transformer,
                 loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class NoTransformOrchestrator(OrchestratorBase):
    def __init__(self,
                 extractor: Extractor,
                 loader: Loader):
        self.loader = loader
        self.extractor = extractor

    def execute(self):
        df = self.extractor.read()
        return self.loader.save(df)


class MultipleExtractOrchestrator(OrchestratorBase):
    def __init__(self,
                 extractor: DelegatingExtractor,
                 transformer: Transformer,
                 loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        dataset = self.extractor.read()
        df = self.transformer.process_many(dataset)
        return self.loader.save(df)


class MultipleTransformOrchestrator(OrchestratorBase):
    def __init__(self,
                 extractor: Extractor,
                 transformer: DelegatingTransformer,
                 loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class OrchestratorFactory:
    @staticmethod
    def create(extractor: Extractor,
               transformer: Transformer,
               loader: Loader) -> OrchestratorBase:
        return Orchestrator(extractor, transformer, loader)

    @staticmethod
    def create_for_multiple_sources(extractor: DelegatingExtractor,
                                    transformer: Transformer,
                                    loader: Loader) -> OrchestratorBase:
        return MultipleExtractOrchestrator(extractor, transformer, loader)

    @staticmethod
    def create_for_multiple_transformers(extractor: Extractor,
                                         transformer: DelegatingTransformer,
                                         loader: Loader) -> OrchestratorBase:
        return MultipleTransformOrchestrator(extractor, transformer, loader)

    @staticmethod
    def create_with_no_transformers(extractor: Extractor,
                                    loader: Loader) -> OrchestratorBase:
        return NoTransformOrchestrator(extractor, loader)
