from typing import List


class DeltaTableDifference:
    def __bool__(self):
        raise NotImplementedError()

    def is_different(self):
        return bool(self)

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

    def _tblproperties_alter_statements(self) -> List[str]:
        """return that ALTER TABLE statements that will
        transform the tblproperties of the base tabel into the target"""
        raise NotImplementedError()

    def _schema_alter_statements(self) -> List[str]:
        raise NotImplementedError()

    def _alter_statements_for_new_location(self) -> List[str]:
        """In case we need to change location,
        the alter statements will consist of creating the new target.
        and then pointing the current table at it. The target will be empty."""

        raise NotImplementedError()

    def alter_table_statements(self) -> List[str]:
        """A list to alter statements that will
        ensure that what used to be the base table, becomes the target table"""

        raise NotImplementedError()
