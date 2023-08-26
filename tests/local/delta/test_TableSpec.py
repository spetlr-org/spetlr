import unittest

from pyspark.sql import types as t

from spetlr import Configurator
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from tests.cluster.delta.deltaspec import tables


class TestDeltaTableSpec(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()
        cls.id = c.get("ID")

        c.register("mydb", dict(name="myDeltaTableSpecTestDb{ID}"))

        cls.base = tables.base
        cls.target = tables.target

    def test_01_diff_alter_statements(self):
        Configurator().set_prod()
        forward_diff = self.target.compare_to(self.base)
        self.assertEqual(
            forward_diff.alter_table_statements(),
            [
                "ALTER TABLE mydeltatablespectestdb.table DROP COLUMN (onlyb)",
                "ALTER TABLE mydeltatablespectestdb.table ADD COLUMN (onlyt string "
                'COMMENT "Only in target")',
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN a DROP NOT NULL",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN d SET NOT NULL",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN a COMMENT"
                ' "gains not null"',
                'ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN d COMMENT ""',
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN a FIRST",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN b AFTER a",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN onlyt AFTER d",
                'COMMENT ON mydeltatablespectestdb.table is "Contains useful data"',
            ],
        )

        reverse_diff = self.base.compare_to(self.target)
        self.assertEqual(
            reverse_diff.alter_table_statements(),
            [
                "ALTER TABLE mydeltatablespectestdb.table DROP COLUMN (onlyt)",
                "ALTER TABLE mydeltatablespectestdb.table ADD COLUMN (onlyb int)",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN d DROP NOT NULL",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN a SET NOT NULL",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN d COMMENT "
                '"Whatsupp"',
                'ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN a COMMENT ""',
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN c FIRST",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN d AFTER c",
                "ALTER TABLE mydeltatablespectestdb.table ALTER COLUMN onlyb AFTER d",
                "COMMENT ON mydeltatablespectestdb.table is null",
            ],
        )

    def test_01_check_init_protections(self):
        tbl1 = DeltaTableSpec(
            name="myDeltaTableSpecTestDb{ID}.table",
            schema=t.StructType(
                fields=[
                    t.StructField(name="c", dataType=t.DoubleType()),
                    t.StructField(
                        name="d",
                        dataType=t.StringType(),
                        nullable=False,
                        metadata={"comment": "Whatsupp"},
                    ),
                    t.StructField(name="onlyb", dataType=t.IntegerType()),
                    t.StructField(name="a", dataType=t.IntegerType()),
                    t.StructField(name="b", dataType=t.StringType()),
                ]
            ),
            location="/somewhere/over/the{ID}/rainbow",
        )
        tbl2 = DeltaTableSpec(
            name="mydeltatablespectestdb.table",
            schema=t.StructType(
                fields=[
                    t.StructField(name="c", dataType=t.DoubleType()),
                    t.StructField(
                        name="d",
                        dataType=t.StringType(),
                        nullable=False,
                        metadata={"comment": "Whatsupp"},
                    ),
                    t.StructField(name="onlyb", dataType=t.IntegerType()),
                    t.StructField(name="a", dataType=t.IntegerType()),
                    t.StructField(name="b", dataType=t.StringType()),
                ]
            ),
            location="dbfs:/somewhere/over/the/rainbow",
        )
        Configurator().set_prod()
        self.assertEqual(tbl1.fully_substituted(), tbl2)
