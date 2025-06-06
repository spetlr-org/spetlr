import unittest
from textwrap import dedent

from pyspark.sql import types as t

from spetlr import Configurator
from spetlr.exceptions import NoSuchValueException
from spetlr.schema_manager import SchemaManager
from spetlr.schema_manager.spark_schema import get_schema
from spetlr.sql import SqlExecutor
from tests.local.configurator import (
    sql,
    tables1,
    tables2,
    tables3,
    tables4,
    tables5,
    views,
)


class TestConfigurator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

    def test_01_import_config(self):
        tc = Configurator()
        tc.add_resource_path(tables1)

        tc.reset(debug=True)
        self.assertRegex(tc.table_name("MyFirst"), "first__.*")
        self.assertRegex(tc.table_name("MyAlias"), "first__.*")
        self.assertRegex(tc.table_name("MyForked"), "another")

        tc.reset(debug=False)
        self.assertRegex(tc.table_name("MyFirst"), "first")
        self.assertRegex(tc.table_name("MyAlias"), "first")
        self.assertRegex(tc.table_name("MyForked"), "first")

    def test_02_details(self):
        tc = Configurator()

        tc.reset(debug=True)
        details = tc.get_all_details()
        self.assertRegex(details["MyFirst_path"], "/tmp/my/first__.*")

        tc.reset(debug=False)
        details = tc.get_all_details()
        self.assertRegex(details["MyFirst_path"], "/mnt/my/first")

    def test_03_json(self):
        tc = Configurator()

        with self.assertRaises(KeyError):
            tc.add_resource_path(tables2)

        tc.register("ENV", "dev")
        tc.add_resource_path(tables2)
        tc.reset(debug=True)
        self.assertRegex(tc.table_name("MyThird"), "first__.*")

        self.assertRegex(
            tc.get_all_details()["MyFourth_path"], "/tmp/dev/path/to/delta"
        )

    def test_04_recursing(self):
        tc = Configurator()
        tc.set_prod()
        self.assertEqual(tc.get("MyRecursing", "path"), "/mnt/tables/recursing")

    def test_05_test_self_recursion_detection(self):
        tc = Configurator()
        tc.set_prod()
        with self.assertRaises(ValueError):
            tc.add_resource_path(tables3)

    def test_06_freestyle(self):
        tc = Configurator()
        tc.set_prod()
        tc.add_resource_path(tables4)
        details = tc.get_all_details()
        self.assertTrue("MyFreeTable_eggs", details)
        self.assertEqual(tc.get("MyFreeTable", "bacon"), "")

    def test_07_bare_strings_and_structures(self):
        tc = Configurator()
        tc.set_prod()
        tc.add_resource_path(tables5)
        self.assertEqual(tc.get("MyPlainLiteral"), "Bar")
        self.assertEqual(tc.get_all_details()["MyCompositeLiteral"], "FooBar")
        st = SchemaManager().get_schema("MyComposite")
        self.assertEqual(
            st,
            t.StructType(
                [
                    t.StructField("a", t.IntegerType()),
                    t.StructField("b", t.DecimalType(12, 2)),
                ]
            ),
        )
        self.assertEqual(tc.table_name("MyComposite"), "ottoBar")

    def test_08_test_deprecated_import(self):
        from spetlr import Configurator
        from spetlr.config_master import TableConfigurator

        tc = TableConfigurator()
        self.assertEqual(tc.table_name("MyComposite"), "ottoBar")

        c = Configurator()

        self.assertIs(tc, c)

    def test_09_configure_from_sql(self):
        c = Configurator()
        c.clear_all_configurations()
        c.add_sql_resource_path(sql)
        c.set_prod()

        self.assertEqual(c.get("MySparkDb", "name"), "my_db1")
        self.assertEqual(c.get("MySparkDb", "path"), "/tmp/foo/bar/my_db1/")
        self.assertEqual(c.get("MySparkDb", "format"), "db")

        self.assertEqual(c.get("MySqlTable", "name"), "my_db1.tbl1")
        c.set_debug()
        self.assertEqual(c.get("MySqlTable", "path"), "/tmp/foo/bar/my_db1/tbl1/")

        self.assertEqual(c.get("MySqlTable", "format"), "delta")
        self.assertEqual(
            c.get("MySqlTable", "options"), {"key1": "val1", "key2": "val2"}
        )
        self.assertEqual(c.get("MySqlTable", "partitioned_by"), ["a", "b"])
        self.assertEqual(c.get("MySqlTable", "cluster_by"), ["a", "b"])
        self.assertEqual(
            c.get("MySqlTable", "clustered_by"),
            dict(
                cols=["c", "d"],
                sorted_by=[
                    dict(name="a", ordering="ASC"),
                    dict(name="b", ordering="DESC"),
                ],
                buckets=5,
            ),
        )
        self.assertEqual(c.get("MySqlTable", "comment"), "Dummy Database 1 table 1")
        self.assertEqual(
            c.get("MySqlTable", "tblproperties"),
            {"key1": "val1", "key2": "val2", "my.key.3": "true"},
        )
        self.assertEqual(
            SchemaManager().get_schema("MySqlTable"),
            get_schema("""a int, b int, c string, d timestamp"""),
        )

        self.assertEqual(
            SchemaManager().get_schema_as_string("MyDetailsTable"),
            """a int, b int, c string, d timestamp, another int""",
        )

        c.set_prod()
        statements = list(SqlExecutor(sql).get_statements("*"))
        self.assertEqual(len(statements), 3)
        self.assertEqual(
            statements[1],
            dedent(
                """\


                        -- spetlr.Configurator key: MyDetailsTable
                        CREATE TABLE IF NOT EXISTS my_db1.details
                        (
                          a int,
                          b int,
                          c string,
                          d timestamp,
                          another int
                          -- comment with ;
                        )
                        USING DELTA
                        COMMENT "Dummy Database 1 details"
                        LOCATION "/mnt/foo/bar/my_db1/details/";"""
            ),
        )

    def test_10_generate_new_UUID_debug(self):
        """
        The UUID should be regenerated
        when applying .regenerate_unique_id_and_clear_conf(),
        and the Configurator is in debug mode.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()

        first_extension = c.get_all_details()["ID"]
        c.regenerate_unique_id_and_clear_conf()
        second_extension = c.get_all_details()["ID"]

        self.assertNotEqual(first_extension, second_extension)

    def test_11_generate_new_UUID_prod(self):
        """
        The UUID should be regenerated
        when applying .regenerate_unique_id_and_clear_conf(),
        and the Configurator is in debug mode.

        But, in production mode, the ID should still be empty string.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.set_prod()

        first_extension = c.get_all_details()["ID"]
        c.regenerate_unique_id_and_clear_conf()
        second_extension = c.get_all_details()["ID"]

        self.assertEqual(first_extension, "")
        self.assertEqual(second_extension, "")

    def test_12_define_nameless(self):
        """The value of the tableId may not be interesting.
        When defined in this way, the `tbl` object can be
        inspected with IntelliSense."""

        c = Configurator()
        c.clear_all_configurations()
        c.set_prod()

        tbl = c.define(name="MyDb.MyTable{ID}", path="/mnt/path/to{ID}/data")

        self.assertEqual(c.get(tbl, "name"), "MyDb.MyTable")

    def test_13_test_keyof(self):
        """Test the ability to extract the key of an object for which only
        the defined name is known."""
        c = Configurator()
        c.clear_all_configurations()

        c.register("MySecretKey", {"name": "MyDb.MyTable{ID}", "path": "/mnt/to/data"})

        key = c.key_of("name", "MyDb.MyTable{ID}")

        self.assertEqual(key, "MySecretKey")

        # key not captured
        c.define(name="anotherName", path="/somewhere")
        # recover  key
        key = c.key_of("name", "anotherName")
        self.assertEqual(c.get(key, "path"), "/somewhere")

    def test_14_views(self):
        c = Configurator()
        c.add_sql_resource_path(views)

        self.assertEqual(c.get("MyViewId", "name"), "SomeViewName")

    def test_15_update_registration(self):
        c = Configurator()
        c.add_sql_resource_path(views)
        # the view is registered as expected
        self.assertEqual(c.get("MyViewId", "name"), "SomeViewName")

        # we register additional details for the view
        # This could be in a yaml file from before the details were put in sql
        c.register("MyViewId", {"my aux detail": "MyValue"})

        # the view is still there
        self.assertEqual(c.get("MyViewId", "name"), "SomeViewName")
        # and the extra detail also
        self.assertEqual(c.get("MyViewId", "my aux detail"), "MyValue")

    def test_16_get_default_value_when_key_is_missing(self):
        c = Configurator()
        c.clear_all_configurations()

        c.register("MyTable", {"name": "MyDb.MyTable{ID}", "path": "/mnt/to/data"})

        # test return None when default is None for missing property
        self.assertEqual(c.get("MyTable", "Missing_Property", None), None)

    def test_17_get_exception_when_key_is_missing(self):
        c = Configurator()
        c.clear_all_configurations()

        c.register("MyTable", {"name": "MyDb.MyTable{ID}", "path": "/mnt/to/data"})

        # test exception for missing property
        with self.assertRaises(NoSuchValueException):
            c.get("MyTable", "Missing_Property")

    def test_18_unregister_key(self):
        """
        Testing that if a keyvalue is None,
        the key in the dict gets unregistered

        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("Test", dict(path="somepath", name="somename"))

        self.assertEqual(c.get("Test", "name"), "somename")
        self.assertEqual(c.get("Test", "path"), "somepath")

        c.register("Test", dict(path=None))

        self.assertEqual(c.get("Test", "name"), "somename")
        # test exception for missing property
        with self.assertRaises(NoSuchValueException):
            self.assertEqual(c.get("Test", "path"), "somepath")

    def test_19_unregister_key_not_found(self):
        """
        Testing that if a keyvalue is None,
        the key in the dict gets unregistered.

        Also, if the path is not set first

        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("Test2", dict(name="somename"))

        self.assertEqual(c.get("Test2", "name"), "somename")

        c.register("Test2", dict(path=None))

        self.assertEqual(c.get("Test2", "name"), "somename")
        # test exception for missing property
        with self.assertRaises(NoSuchValueException):
            self.assertEqual(c.get("Test2", "path"), "somepath")

    def test_20_unregister_multiple_keys(self):
        """
        Testing that multiple key-value pairs set to None are removed
        from the dictionary.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("TestMulti", dict(name="table1", path="/mnt/table1", format="delta"))

        self.assertEqual(c.get("TestMulti", "name"), "table1")
        self.assertEqual(c.get("TestMulti", "path"), "/mnt/table1")
        self.assertEqual(c.get("TestMulti", "format"), "delta")

        c.register("TestMulti", dict(path=None, format=None))

        self.assertEqual(c.get("TestMulti", "name"), "table1")
        with self.assertRaises(NoSuchValueException):
            c.get("TestMulti", "path")
        with self.assertRaises(NoSuchValueException):
            c.get("TestMulti", "format")

    def test_21_unregister_all_keys(self):
        """
        Testing that if all key-value pairs are set to None,
        the entire entry is removed.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("TestAll", dict(name="table2", path="/mnt/table2"))

        self.assertEqual(c.get("TestAll", "name"), "table2")
        self.assertEqual(c.get("TestAll", "path"), "/mnt/table2")

        c.register("TestAll", dict(name=None, path=None))

        with self.assertRaises(KeyError):
            c.get("TestAll", "name")
        with self.assertRaises(KeyError):
            c.get("TestAll", "path")

    def test_22_unregister_partial_update(self):
        """
        Testing that if only one key is set to None, others remain unchanged.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("TestPartial", dict(name="table3", path="/mnt/table3"))

        self.assertEqual(c.get("TestPartial", "name"), "table3")
        self.assertEqual(c.get("TestPartial", "path"), "/mnt/table3")

        c.register("TestPartial", dict(path=None))

        self.assertEqual(c.get("TestPartial", "name"), "table3")
        with self.assertRaises(NoSuchValueException):
            c.get("TestPartial", "path")

    def test_23_unregister_key_then_readd(self):
        """
        Testing that after a key-value pair is removed,
        it can be added back successfully.
        """

        c = Configurator()
        c.clear_all_configurations()
        c.register("TestReAdd", dict(name="table4", path="/mnt/table4"))

        self.assertEqual(c.get("TestReAdd", "name"), "table4")
        self.assertEqual(c.get("TestReAdd", "path"), "/mnt/table4")

        c.register("TestReAdd", dict(path=None))

        with self.assertRaises(NoSuchValueException):
            c.get("TestReAdd", "path")

        c.register("TestReAdd", dict(path="/mnt/new_table4"))

        self.assertEqual(c.get("TestReAdd", "path"), "/mnt/new_table4")

    def test_24_unregister_does_not_affect_other_keys(self):
        """
        Testing that removing a key from one entry does not
        affect other unrelated keys in the dictionary.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("TestA", dict(name="tableA", path="/mnt/tableA"))
        c.register("TestB", dict(name="tableB", path="/mnt/tableB"))

        self.assertEqual(c.get("TestA", "name"), "tableA")
        self.assertEqual(c.get("TestA", "path"), "/mnt/tableA")
        self.assertEqual(c.get("TestB", "name"), "tableB")
        self.assertEqual(c.get("TestB", "path"), "/mnt/tableB")

        c.register("TestA", dict(path=None))

        with self.assertRaises(NoSuchValueException):
            c.get("TestA", "path")

        # Ensure TestB is not affected
        self.assertEqual(c.get("TestB", "name"), "tableB")
        self.assertEqual(c.get("TestB", "path"), "/mnt/tableB")

    def test_25_unregister_last_key_removes_entry(self):
        """
        Testing that if the last remaining key-value pair is set to None,
        the entire entry is removed from the dictionary.
        """
        c = Configurator()
        c.clear_all_configurations()
        c.register("somekey", dict(path="hey"))

        self.assertEqual(c.get("somekey", "path"), "hey")

        c.register("somekey", dict(path=None))

        # Ensure the key is completely removed
        with self.assertRaises(KeyError):
            c.get("somekey", "path")

        # Ensure the key is completely removed
        with self.assertRaises(KeyError):
            c.get("somekey")

        # Ensure "somekey" itself is removed from raw details
        self.assertNotIn("somekey", c._raw_resource_details)

    def test_26_register_key_with_no_values(self):
        """
        Tests that if one tries to register a key
        with no values, TypeError is raised
        """

        c = Configurator()
        c.clear_all_configurations()

        with self.assertRaises(TypeError) as cm:
            c.register("somekey")

        self.assertIn("missing 1 required positional argument", str(cm.exception))
