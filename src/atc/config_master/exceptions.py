class SqlParseException(Exception):
    pass


class NoCreationException(SqlParseException):
    pass


class UnknownCreationException(SqlParseException):
    pass
