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
        cls.id = c.get("ID")

        c.register("mydb", dict(name="myDeltaTableSpecTestDb{ID}"))
        cls.base = tables.base
        cls.target = tables.target

    @unittest.skipUnless(
        Spark.version() >= Spark.DATABRICKS_RUNTIME_11_3,
        "Drop column only supported from DBR 11.0",
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
