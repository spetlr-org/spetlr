import deprecation

from atc.configurator.configurator import Configurator


@deprecation.deprecated(
    deprecated_in="1.0.51",
    removed_in="2.0.0",
    details="Use atc.Configurator instead.",
)
class TableConfigurator(Configurator):
    pass
