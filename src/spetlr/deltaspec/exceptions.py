from spetlr.exceptions import SpetlrException


class InvalidSpecificationError(SpetlrException):
    """Internally this exception is used when attempting
    to unpack configurations into table and db specifications and this fails"""

    pass


class NoTableAtTarget(InvalidSpecificationError):
    """Indiactes thate there is not table at the target location."""

    pass


class TableSpecNotReadable(SpetlrException):
    """A read was attempted, but the table is not readable as specified."""

    pass
