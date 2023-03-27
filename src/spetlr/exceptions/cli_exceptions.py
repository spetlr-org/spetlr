from spetlr.exceptions import SpetlrException


class SpetlrCliException(SpetlrException):
    pass


class SpetlrCliCheckFailed(SpetlrCliException):
    pass
