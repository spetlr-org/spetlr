from atc.atc_exceptions import AtcException


class SqlParseException(AtcException):
    pass


class NoCreationException(SqlParseException):
    pass


class UnknownCreationException(SqlParseException):
    pass


class UnknownShapeException(AtcException):
    pass
