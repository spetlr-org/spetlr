from typing import Protocol

from atc.delta import DeltaHandle


class ThMaker(Protocol):
    """Use this as a type when you need either DeltaHandle or a SqlServer instance."""

    def from_tc(self, id: str) -> DeltaHandle:
        pass
