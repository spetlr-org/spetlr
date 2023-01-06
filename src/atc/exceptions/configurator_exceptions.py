from . import AtcException


class AtcConfiguratorException(AtcException):
    pass


class AtcConfiguratorInvalidSqlException(AtcConfiguratorException):
    pass


class AtcConfiguratorInvalidSqlCommentsException(AtcConfiguratorException):
    pass
