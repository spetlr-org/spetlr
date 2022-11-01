from deprecated import deprecated

from atc.configurator.configurator import Configurator


@deprecated(
    reason="Don't import from atc.config_master. Use: from atc import Configurator.",
)
class TableConfigurator(Configurator):
    pass
