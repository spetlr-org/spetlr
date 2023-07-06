import unittest

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_sql_code_to_config
from spetlr.deltaspec.DeltaDatabaseSpec import DeltaDatabaseSpec
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.spark import Spark


class TestTableSpec(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.base = DeltaTableSpec.from_sql(
            """CREATE TABLE myDeltaTableSpecTestDb{ID}.table
                (

                    c double,
                    d string NOT NULL COMMENT "Whatsupp",
                    onlyb int,
                    a int,
                    b string,
                )
                USING DELTA
                LOCATION "/somewhere/over/the/rainbow"
            """
        )
        cls.target = DeltaTableSpec.from_sql(
            """CREATE TABLE myDeltaTableSpecTestDb{ID}.table
        (
            a int NOT NULL COMMENT "gains not null",
            b string,
            c double,
            d string,
            onlyt string COMMENT "Only in target",
        )
        USING DELTA
        COMMENT "Contains useful data"
        LOCATION "/somewhere/over/the/rainbow"
        """
        )

        c = Configurator()
        c.set_debug()
        cls.id = c.get('ID')


    def test_01_diff_alter_statements(self):
        forward_diff = self.target.compare_to(self.base)
        self.assertEqual(
            forward_diff.alter_table_statements(),
            [
                "ALTER TABLE myDeltaTableSpecTestDb.table DROP COLUMN (onlyb)",
                "ALTER TABLE myDeltaTableSpecTestDb.table ADD COLUMN (onlyt string "
                'COMMENT "Only in target")',
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a DROP NOT NULL",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d SET NOT NULL",
                'ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a COMMENT "gains '
                'not null"',
                'ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d COMMENT ""',
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a FIRST",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN b AFTER a",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN onlyt AFTER d",
                'COMMENT ON myDeltaTableSpecTestDb.table is "Contains useful data"',
            ],
        )

        reverse_diff = self.base.compare_to(self.target)
        self.assertEqual(
            reverse_diff.alter_table_statements(),
            [
                "ALTER TABLE myDeltaTableSpecTestDb.table DROP COLUMN (onlyt)",
                "ALTER TABLE myDeltaTableSpecTestDb.table ADD COLUMN (onlyb int)",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d DROP NOT NULL",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a SET NOT NULL",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d COMMENT "
                '"Whatsupp"',
                'ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a COMMENT ""',
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN c FIRST",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d AFTER c",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN onlyb AFTER d",
                "COMMENT ON myDeltaTableSpecTestDb.table is null",
            ],
        )
