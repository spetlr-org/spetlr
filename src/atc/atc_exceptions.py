class AtcException(Exception):
    pass


class NoTableException(AtcException):
    value = "No table found!"
    pass


class UnkownPathException(AtcException):
    value = "Something went wrong during reading of path!"
    pass
