class AtcException(Exception):
    pass


class NoTableException(AtcException):
    value = "No table found!"
    pass
