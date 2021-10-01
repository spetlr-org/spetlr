from .extractor import Extractor, DelegatingExtractor
from .loader import Loader
from .transformer import Transformer, DelegatingTransformer


class Orchestrator:
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
        self.loader.save(df)


class MultipleExtractOrchestrator:
    def __init__(self,
                 extractor: DelegatingExtractor,
                 transformer: DelegatingTransformer,
                 loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        dataset = self.extractor.read()
        df = self.transformer.process(dataset)
        self.loader.save(df)


class OrchestratorFactory:
    @staticmethod
    def create(extractor: Extractor, transformer: Transformer, loader: Loader):
        return Orchestrator(extractor, transformer, loader)

    @staticmethod
    def create_for_multiple_sources(extractor: DelegatingExtractor,
                                    transformer: DelegatingTransformer,
                                    loader: Loader):
        return MultipleExtractOrchestrator(extractor, transformer, loader)
