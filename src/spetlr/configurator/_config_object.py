class ConfigObject:
    def __init__(self, key: str):
        import spetlr  # local import to prevent circular import

        self.configurator = spetlr.Configurator()
        self.key = key

    def __str__(self):
        return self.key

    def __repr__(self):
        return f"ConfigObject({repr(self.key)})"

    def __getattr__(self, item):
        try:
            return self.configurator.get(self.key, item)
        except:  # noqa
            raise AttributeError()
