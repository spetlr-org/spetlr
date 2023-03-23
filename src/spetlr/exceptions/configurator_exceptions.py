from . import SpetlrException


class SpetlrConfiguratorException(SpetlrException):
    pass


class SpetlrConfiguratorInvalidSqlException(SpetlrConfiguratorException):
    pass


class SpetlrConfiguratorInvalidSqlCommentsException(SpetlrConfiguratorException):
    pass
