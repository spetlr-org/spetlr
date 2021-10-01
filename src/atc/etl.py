class Extractor:
    def __init__(self):
        pass

    def read(self):
        return None

class Transformer:
    def __init__(self):
        pass

    def process(self, df):
        return df

class Loader:
    def __init__(self):
        pass

    def save(self, df):
        return df

class Orchestrator:
    def __init__(self,
                 extractor: Extractor,
                 transformer: Transformer,
                 loader: Loader):
        self.loader = loader
        self.transformer = transformer
        self.extractor = extractor

    @staticmethod
    def create(extractor: Extractor, transformer: Transformer, loader: Loader):
        return Orchestrator(extractor, transformer, loader)

    def execute(self):
        df = self.extractor.read()
        df = self.transformer.process(df)
        self.loader.save(df)
