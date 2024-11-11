# These tests will be activated when the corresponding cluster tests get fixed

# import dataclasses
# import unittest
# from textwrap import dedent

# from pyspark.sql import types as t

# from spetlr import Configurator
# from spetlr.deltaspec import DeltaTableSpecDifference, TableSpectNotEnforcable
# from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
# from tests.cluster.delta.deltaspec import tables
# from tests.local.delta import sql


# class TestDeltaTableSpec(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         c = Configurator()
#         c.clear_all_configurations()
#         c.set_prod()

#         cls.base = tables.base
#         cls.target = tables.target

#     def test_01_create_statements(self):
#         self.assertEqual(
#             self.base.get_sql_create(),
#             dedent(
#                 """\
#                 CREATE TABLE myDeltaTableSpecTestDb{ID}.tbl
#                 (
#                   c double,
#                   d string COMMENT "Whatsupp",
#                   onlyb int,
#                   a int,
#                   b int
#                 )
#                 USING DELTA
#                 LOCATION "dbfs:/tmp/somewhere{ID}/over/the/rainbow"
#                 TBLPROPERTIES (
#                   "delta.columnMapping.mode" = "name",
#                   "delta.minReaderVersion" = "2",
#                   "delta.minWriterVersion" = "5"
#                 )
#                 """
#             ),
#         )

#     def test_02_alter_statements_raise(self):
#         Configurator().set_prod()
#         forward_diff = self.target.compare_to(self.base)
#         with self.assertRaises(TableSpectNotEnforcable):
#             forward_diff.alter_statements(
#                 allow_columns_add=False,
#                 allow_columns_type_change=True,
#                 allow_columns_drop=True,
#                 allow_columns_reorder=True,
#             )

#         with self.assertRaises(TableSpectNotEnforcable):
#             forward_diff.alter_statements(
#                 allow_columns_add=True,
#                 allow_columns_type_change=False,
#                 allow_columns_drop=True,
#                 allow_columns_reorder=True,
#             )

#         with self.assertRaises(TableSpectNotEnforcable):
#             forward_diff.alter_statements(
#                 allow_columns_add=True,
#                 allow_columns_type_change=True,
#                 allow_columns_drop=True,
#                 allow_columns_reorder=False,
#             )
#         print("No raise when they are all warnings")
#         forward_diff.alter_statements(errors_as_warnings=True)

#     def test_03_diff_alter_statements(self):
#         Configurator().set_prod()
#         forward_diff = self.target.compare_to(self.base)

#         self.assertEqual(
#             forward_diff.alter_statements(
#                 allow_columns_add=True,
#                 allow_columns_type_change=True,
#                 allow_columns_drop=True,
#                 allow_columns_reorder=True,
#             ),
#             [
#                 "ALTER TABLE mydeltatablespectestdb.tbl "
#                 'SET TBLPROPERTIES ("my.cool.peoperty" = "bacon")',
#                 "ALTER TABLE mydeltatablespectestdb.tbl DROP COLUMNS (b, onlyb)",
#                 dedent(
#                     """\
#                 ALTER TABLE mydeltatablespectestdb.tbl ADD COLUMNS (
#                   b string,
#                   onlyt string COMMENT "Only in target"
#                 )"""
#                 ),
#                 # "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN a DROP NOT NULL",
#                 # "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN d SET NOT NULL",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN a COMMENT"
#                 ' "gains not null"',
#                 'ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN d COMMENT ""',
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN a FIRST",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN b AFTER a",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN onlyt AFTER d",
#                 'COMMENT ON TABLE mydeltatablespectestdb.tbl is "Contains useful data"',
#             ],
#         )

#         reverse_diff = self.base.compare_to(self.target)
#         self.assertEqual(
#             reverse_diff.alter_statements(
#                 allow_columns_add=True,
#                 allow_columns_type_change=True,
#                 allow_columns_drop=True,
#                 allow_columns_reorder=True,
#                 allow_name_change=True,
#                 allow_location_change=True,
#             ),
#             [
#                 "ALTER TABLE mydeltatablespectestdb.tbl "
#                 'UNSET TBLPROPERTIES ("my.cool.peoperty")',
#                 "ALTER TABLE mydeltatablespectestdb.tbl DROP COLUMNS (b, onlyt)",
#                 dedent(
#                     """\
#                 ALTER TABLE mydeltatablespectestdb.tbl ADD COLUMNS (
#                   onlyb int,
#                   b int
#                 )"""
#                 ),
#                 # "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN d DROP NOT NULL",
#                 # "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN a SET NOT NULL",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN d COMMENT "
#                 '"Whatsupp"',
#                 'ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN a COMMENT ""',
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN c FIRST",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN d AFTER c",
#                 "ALTER TABLE mydeltatablespectestdb.tbl ALTER COLUMN onlyb AFTER d",
#                 "COMMENT ON TABLE mydeltatablespectestdb.tbl is null",
#             ],
#         )

#     def test_04_check_init_protections(self):
#         tbl1 = DeltaTableSpec(
#             name="myDeltaTableSpecTestDb{ID}.tbl",
#             schema=t.StructType(
#                 fields=[
#                     t.StructField(name="c", dataType=t.DoubleType()),
#                     t.StructField(
#                         name="d",
#                         dataType=t.StringType(),
#                         nullable=False,
#                         metadata={"comment": "Whatsupp"},
#                     ),
#                     t.StructField(name="onlyb", dataType=t.IntegerType()),
#                     t.StructField(name="a", dataType=t.IntegerType()),
#                     t.StructField(name="b", dataType=t.StringType()),
#                 ]
#             ),
#             location="/somewhere/over/the{ID}/rainbow",
#         )
#         tbl2 = DeltaTableSpec(
#             name="spark_catalog.mydeltatablespectestdb.tbl",
#             schema=t.StructType(
#                 fields=[
#                     t.StructField(name="c", dataType=t.DoubleType()),
#                     t.StructField(
#                         name="d",
#                         dataType=t.StringType(),
#                         nullable=False,
#                         metadata={"comment": "Whatsupp"},
#                     ),
#                     t.StructField(name="onlyb", dataType=t.IntegerType()),
#                     t.StructField(name="a", dataType=t.IntegerType()),
#                     t.StructField(name="b", dataType=t.StringType()),
#                 ]
#             ),
#             location="dbfs:/somewhere/over/the/rainbow",
#         )
#         Configurator().set_prod()
#         d = tbl2.compare_to(tbl1.fully_substituted())
#         self.assertFalse(d.is_different(), d)

#     def test_05_namechagne(self):
#         statements = tables.newname.compare_to(tables.oldname).alter_statements(
#             allow_columns_add=True,
#             allow_columns_type_change=True,
#             allow_columns_drop=True,
#             allow_columns_reorder=True,
#             allow_name_change=True,
#             allow_location_change=True,
#         )
#         self.assertEqual(
#             statements,
#             [
#                 "ALTER TABLE mydeltatablespectestdb.namechange_old RENAME TO "
#                 "mydeltatablespectestdb.namechange_new"
#             ],
#         )

#     def test_06_location_change(self):
#         statements = tables.newlocation.compare_to(tables.oldlocation).alter_statements(
#             allow_columns_add=True,
#             allow_columns_type_change=True,
#             allow_columns_drop=True,
#             allow_columns_reorder=True,
#             allow_name_change=True,
#             allow_location_change=True,
#         )
#         self.assertEqual(
#             statements,
#             [
#                 "CREATE TABLE delta.`dbfs:/tmp/somewhere/locchange/new`\n"
#                 "(\n"
#                 "  b string,\n"
#                 "  c double,\n"
#                 "  d string\n"
#                 ")\n"
#                 "USING DELTA\n"
#                 'LOCATION "dbfs:/tmp/somewhere/locchange/new"\n'
#                 'COMMENT "Contains useful data"\n'
#                 "TBLPROPERTIES (\n"
#                 '  "delta.columnMapping.mode" = "name",\n'
#                 '  "delta.minReaderVersion" = "2",\n'
#                 '  "delta.minWriterVersion" = "5"\n'
#                 ")\n",
#                 "ALTER TABLE mydeltatablespectestdb.locchange SET LOCATION "
#                 '"dbfs:/tmp/somewhere/locchange/new"',
#             ],
#         )

#     def test_07_from_tc(self):
#         c = Configurator()
#         c.clear_all_configurations()
#         c.add_sql_resource_path(sql)

#         ds = DeltaTableSpec.from_tc("Table1")
#         self.assertEqual(
#             ds.get_sql_create(),
#             (
#                 "CREATE TABLE sometable\n"
#                 "(\n"
#                 "  a int\n"
#                 ")\n"
#                 "USING DELTA\n"
#                 "TBLPROPERTIES (\n"
#                 '  "delta.columnMapping.mode" = "name",\n'
#                 '  "delta.minReaderVersion" = "2",\n'
#                 '  "delta.minWriterVersion" = "5"\n'
#                 ")\n"
#             ),
#         )

#         # The deltaspec needs to be resolved explicitly
#         ds = DeltaTableSpec.from_tc("Table2")
#         c.set_prod()
#         self.assertEqual(ds.fully_substituted().location, "dbfs:/mnt/foo/bar")
#         # This allows it to be globally initialized before the prod/debug is set
#         c.set_debug()
#         self.assertEqual(ds.fully_substituted().location, "dbfs:/tmp/foo/bar")

#     def test_08_modify_to_allow_drop(self):
#         Configurator().set_prod()
#         # a table has been created without the DeltaTableSpec:
#         base = DeltaTableSpec(
#             name="myDeltaTableSpecTestDb{ID}.direct",
#             schema=t.StructType(
#                 fields=[
#                     t.StructField(name="b", dataType=t.StringType()),
#                     t.StructField(name="c", dataType=t.DoubleType()),
#                     t.StructField(name="d", dataType=t.StringType()),
#                 ]
#             ),
#             tblproperties={
#                 "delta.minReaderVersion": "2",
#                 "delta.minWriterVersion": "3",
#             },
#         )

#         # now a modified version is to be applied that drops columns:
#         ds = DeltaTableSpec.from_sql(tables.simple_modified_sql)
#         diff = ds.compare_to(base)
#         self.assertEqual(
#             diff.alter_statements(allow_columns_drop=True),
#             [
#                 "ALTER TABLE mydeltatablespectestdb.direct SET TBLPROPERTIES "
#                 '("delta.minWriterVersion" = "5", "delta.columnMapping.mode" = "name")',
#                 "ALTER TABLE mydeltatablespectestdb.direct DROP COLUMN (d)",
#             ],
#         )

#     def test_09_generated_and_default_expression(self):
#         c = Configurator()
#         c.clear_all_configurations()
#         c.add_sql_resource_path(sql)
#         ds = DeltaTableSpec.from_tc("Table3")
#         self.assertEqual(
#             ds.schema,
#             t.StructType(
#                 [
#                     t.StructField("id", t.LongType(), True),
#                     t.StructField("a", t.IntegerType(), True),
#                     t.StructField("b", t.IntegerType(), True),
#                     t.StructField("area", t.IntegerType(), True),
#                 ]
#             ),
#         )
#         diff = DeltaTableSpecDifference(
#             target=ds, base=dataclasses.replace(ds, tblproperties={})
#         )
#         # The diff is not a match, tableproperties, mismatch
#         self.assertFalse(diff.complete_match(), diff)

#         # ignore the tblproperties and it matches
#         diff = diff.ignore("tblproperties")
#         self.assertTrue(diff.complete_match(), diff)
