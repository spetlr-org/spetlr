class InvalidSpecificationError(Exception):
    """Internally this exception is used when attempting
    to unpack configurations into table and db specifications and this fails"""

    pass


class NoTableAtTarget(InvalidSpecificationError):
    """Indiactes thate there is not table at the target location."""

    pass
