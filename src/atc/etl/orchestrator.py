from .extractor import Extractor, DelegatingExtractor
from .loader import Loader
from .transformer import Transformer, DelegatingTransformer, MultiInputTransformer


class Orchestrator:
    def __init__(self, extractor: Extractor, transformer: Transformer, loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        df = self.extractor.read()
        df = self.transformer.process(df)
        return self.loader.save(df)


class NoTransformOrchestrator:
    def __init__(self, extractor: Extractor, loader: Loader):
        self.loader = loader
        self.extractor = extractor

    def execute(self):
        df = self.extractor.read()
        return self.loader.save(df)


class MultipleExtractOrchestrator:
    def __init__(self, extractor: DelegatingExtractor, transformer: MultiInputTransformer, loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    def execute(self):
        dataset = self.extractor.read()
        df = self.transformer.process_many(dataset)
        return self.loader.save(df)


class MultipleTransformOrchestrator:
    def __init__(self, extractor: Extractor, transformer: DelegatingTransformer, loader: Loader):
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
               loader: Loader) -> Orchestrator:
        return Orchestrator(extractor, transformer, loader)

    @staticmethod
    def create_for_multiple_sources(extractors: [Extractor],
                                    transformer: MultiInputTransformer,
                                    loader: Loader) -> MultipleExtractOrchestrator:
        return MultipleExtractOrchestrator(DelegatingExtractor(extractors), transformer, loader)

    @staticmethod
    def create_for_multiple_transformers(extractor: Extractor,
                                         transformers: [Transformer],
                                         loader: Loader) -> MultipleTransformOrchestrator:
        return MultipleTransformOrchestrator(extractor, DelegatingTransformer(transformers), loader)

    @staticmethod
    def create_with_no_transformers(extractor: Extractor,
                                    loader: Loader) -> NoTransformOrchestrator:
        return NoTransformOrchestrator(extractor, loader)
