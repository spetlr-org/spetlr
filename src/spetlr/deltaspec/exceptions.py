from spetlr.exceptions import SpetlrException


class InvalidSpecificationError(SpetlrException):
    """Internally this exception is used when attempting
    to unpack configurations into table and db specifications and this fails"""


class NoTableAtTarget(InvalidSpecificationError):
    """Indiactes thate there is not table at the target location."""


class TableSpectNotEnforcable(SpetlrException):
    """For some reason it was not possible to make the storage match the spec."""


class TableSpecNotReadable(SpetlrException):
    """A read was attempted, but the table is not readable as specified."""


class TableSpecSchemaMismatch(SpetlrException):
    """A write was attempted, but the dataframe is not as specified."""


class InvalidTableName(SpetlrException):
    """A table name of the given components is invalid."""
