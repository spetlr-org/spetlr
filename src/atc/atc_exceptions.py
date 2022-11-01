class AtcException(Exception):
    pass


class AtcKeyError(KeyError):
    pass


class NoTableException(AtcException):
    value = "No table found!"
    pass


class UnkownPathException(AtcException):
    value = "Something went wrong during reading of path!"
    pass


class ColumnDoesNotExistException(AtcException):
    pass


class MoreThanTwoDataFramesException(AtcException):
    pass


class EhJsonToDeltaException(AtcException):
    pass
