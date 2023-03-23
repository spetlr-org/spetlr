from deprecated import deprecated

from spetlr.configurator.configurator import Configurator


@deprecated(
    reason="Don't import from spetlr.config_master. Use: from atc import Configurator.",
)
class TableConfigurator(Configurator):
    pass
