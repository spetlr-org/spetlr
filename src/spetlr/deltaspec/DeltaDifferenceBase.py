from typing import List


class DeltaDifferenceBase:
    def nullbase(self) -> bool:
        """is the comparison to a null base. Meaning there is no table."""
        pass

    def is_different(self) -> bool:
        raise NotImplementedError()

    def complete_match(self) -> bool:
        raise NotImplementedError()

    def name_match(self) -> bool:
        raise NotImplementedError()

    def location_match(self) -> bool:
        raise NotImplementedError()

    def schema_match(self) -> bool:
        """True if the schema is identical between the tables."""
        raise NotImplementedError()

    def metadata_match(self) -> bool:
        raise NotImplementedError()

    def is_readable(self) -> bool:
        raise NotImplementedError()

    def schema_is_selectable(self) -> bool:
        raise NotImplementedError()

    def alter_statements(self, allow_new_columns=False) -> List[str]:
        """A list to alter statements that will
        ensure that what used to be the base, becomes the target"""
        raise NotImplementedError()
