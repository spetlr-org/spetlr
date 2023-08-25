import unittest

from spetlr import Configurator
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.spark import Spark


class TestTableSpec(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()
        cls.id = c.get("ID")

        c.register("mydb", dict(name="myDeltaTableSpecTestDb{ID}"))

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

    def test_01_diff_alter_statements(self):
        Configurator().set_prod()
        forward_diff = self.target.compare_to(self.base)
        self.assertEqual(
            forward_diff.alter_table_statements(),
            [
                "ALTER TABLE myDeltaTableSpecTestDb.table DROP COLUMN (onlyb)",
                "ALTER TABLE myDeltaTableSpecTestDb.table ADD COLUMN (onlyt string "
                'COMMENT "Only in target")',
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a DROP NOT NULL",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN d SET NOT NULL",
                "ALTER TABLE myDeltaTableSpecTestDb.table ALTER COLUMN a COMMENT"
                ' "gains not null"',
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

    def test_02_execute_alter_statements(self):
        Configurator().set_debug()
        spark = Spark.get()
        spark.sql(
            f"""
            CREATE DATABASE {Configurator().get('mydb','name')};
        """
        )

        # at first the table does not exist
        diff = self.base.compare_to_storage()
        self.assertTrue(diff.is_different(), diff)

        # then we make it exist
        self.base.make_storage_match()

        # not it exists and matches
        diff = self.base.compare_to_storage()
        self.assertFalse(diff.is_different(), repr(diff))

        # but it does not match the target
        diff = self.target.compare_to_storage()
        self.assertTrue(diff.is_different(), repr(diff))

        # so make the target exist
        self.target.make_storage_match()

        # now the base no longer matches
        diff = self.base.compare_to_storage()
        self.assertTrue(diff.is_different(), diff)

        # but the target matches.
        diff = self.target.compare_to_storage()
        self.assertFalse(diff.is_different(), repr(diff))

        # clean up after test.
        spark.sql(
            f"""
            DROP DATABASE {Configurator().get('mydb','name')} CASCADE;
            """
        )
