import deprecation

from atc.configurator.configurator import Configurator

TableConfigurator = deprecation.deprecated(
    details="Don't import from atc.config_master. Use: from atc import Configurator.",
)(Configurator)
