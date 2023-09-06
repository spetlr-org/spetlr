import unittest

from spetlr import Configurator
from spetlr.spark import Spark
from tests.cluster.delta.deltaspec import tables


class TestTableSpec(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()

        c.register("mydb", dict(name="myDeltaTableSpecTestDb{ID}"))
        cls.base = tables.base
        cls.target = tables.target

    @unittest.skipUnless(
        Spark.version() >= Spark.DATABRICKS_RUNTIME_11_3,
        "Drop column only supported from DBR 11.0",
    )
    def test_tblspec(self):
        c = Configurator()
        c.set_debug()

        spark = Spark.get()
        db = c.get("mydb", "name")
        spark.sql(f"CREATE DATABASE {db};")

        # at first the table does not exist
        diff = self.base.compare_to_name()
        self.assertTrue(diff.is_different(), diff)
        self.assertTrue(diff.nullbase(), diff)

        # then we make it exist
        self.base.make_storage_match()

        # now it exists and matches
        diff = self.base.compare_to_name()
        self.assertFalse(diff.is_different(), repr(diff))

        # but it does not match the target
        diff = self.target.compare_to_name()
        # the names are the same
        self.assertTrue(diff.name_match(), repr(diff))
        # the rest of the table is not the same
        self.assertTrue(diff.is_different(), repr(diff))

        # overwite to the target schema
        df = Spark.get().createDataFrame([(1, "a", 3.14, "b", "c")], self.target.schema)
        self.target.overwrite(df)

        # now the base no longer matches
        diff = self.base.compare_to_name()
        self.assertTrue(diff.is_different(), diff)

        # but the target matches.
        diff = self.target.compare_to_name()
        self.assertFalse(diff.is_different(), repr(diff))

        # clean up after test.
        spark.sql(f"DROP DATABASE {db} CASCADE")
