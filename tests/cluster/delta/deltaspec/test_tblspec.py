## Commenting out as these tests are not working as expected in UC-enabled clusters.


# import unittest

# from spetlrtools.testing import DataframeTestCase

# from spetlr import Configurator
# from spetlr.deltaspec import DeltaTableSpec
# from spetlr.spark import Spark
# from tests.cluster.delta.deltaspec import tables


# @unittest.skipUnless(
#     Spark.version() >= Spark.DATABRICKS_RUNTIME_11_3,
#     "Drop column only supported from DBR 11.0",
# )
# class TestTableSpec(DataframeTestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         c = Configurator()
#         c.clear_all_configurations()
#         c.set_debug()

#         c.register("mydb", dict(name="myDeltaTableSpecTestDb{ID}"))
#         cls.base = tables.base
#         cls.target = tables.target

#         db = c.get("mydb", "name")
#         Spark.get().sql(f"CREATE DATABASE {db};")

#     @classmethod
#     def tearDownClass(cls) -> None:
#         c = Configurator()
#         db = c.get("mydb", "name")
#         # clean up after test.
#         Spark.get().sql(f"DROP DATABASE {db} CASCADE")

#     def test_01_tblspec(self):
#         # at first the table does not exist
#         diff = self.base.compare_to_name()
#         self.assertTrue(diff.is_different(), diff)
#         self.assertTrue(diff.nullbase(), diff)

#         # then we make it exist
#         self.base.make_storage_match()

#         # now it exists and matches
#         diff = self.base.compare_to_name()
#         self.assertFalse(diff.is_different(), repr(diff))

#         # but it does not match the target
#         diff = self.target.compare_to_name()
#         # the names are the same
#         self.assertTrue(diff.name_match(), repr(diff))
#         # the rest of the table is not the same
#         self.assertTrue(diff.is_different(), repr(diff))

#         # the table is not readable because of schema mismatch
#         self.assertFalse(self.target.is_readable())

#         # overwriting is possible and updates to the target schema
#         df = Spark.get().createDataFrame([(1, "a", 3.14, "b", "c")], self.target.schema)
#         self.target.make_storage_match(errors_as_warnings=True)

#         self.target.get_dh().overwrite(df, overwriteSchema=True)

#         # now the base no longer matches
#         diff = self.base.compare_to_name()
#         self.assertTrue(diff.is_different(), diff)

#         # but the target matches.
#         diff = self.target.compare_to_name()
#         self.assertFalse(diff.is_different(), repr(diff))

#     def test_02_location_change(self):
#         tables.oldlocation.make_storage_match(allow_table_create=True)

#         # location mismatch makes the tables not readable
#         self.assertFalse(tables.newlocation.is_readable())

#         # overwriting will update the table location.
#         tables.newlocation.make_storage_match(allow_location_change=True)

#         diff = tables.newlocation.compare_to_name()
#         self.assertTrue(diff.complete_match(), diff)
#         self.assertTrue(tables.newlocation.is_readable(), diff)

#     def test_03_name_change(self):
#         tables.oldname.make_storage_match(allow_table_create=True)

#         spark = Spark.get()
#         df = spark.createDataFrame([("eggs", 3.5, "spam")], tables.oldname.schema)
#         tables.oldname.get_dh().overwrite(df)

#         for stmt in tables.newname.compare_to(tables.oldname).alter_statements(
#             allow_name_change=True
#         ):
#             spark.sql(stmt)

#         diff = tables.newname.compare_to_name()
#         self.assertTrue(diff.complete_match(), diff)
#         self.assertEqual(tables.newname.get_dh().read().count(), 1)

#     def test_04_can_control_managed_table(self):
#         tables.managed.make_storage_match(allow_table_create=True)

#         diff = tables.managed.compare_to_name()
#         self.assertTrue(diff.complete_match(), diff)

#     def test_05_modify_to_allow_drop(self):
#         # a table has been created without the DeltaTableSpec:
#         spark = Spark.get()
#         spark.sql(tables.simple_create_sql.format(**Configurator().get_all_details()))

#         # now a modified version is to be applied that drops columns:
#         ds = DeltaTableSpec.from_sql(tables.simple_modified_sql)
#         ds.make_storage_match(allow_columns_drop=True)

#         diff = ds.compare_to_name()
#         self.assertTrue(diff.complete_match(), diff)
