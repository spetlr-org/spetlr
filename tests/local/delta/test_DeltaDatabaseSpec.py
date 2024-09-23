# These tests will be activated when the corresponding cluster tests get fixed


# import unittest

# from spetlr import Configurator
# from spetlr.configurator.sql.parse_sql import parse_sql_code_to_config
# from spetlr.deltaspec.DeltaDatabaseSpec import DeltaDatabaseSpec


# class TestTableSpecConversions(unittest.TestCase):
#     def test_DeltaDatabaseSpec_from_sql(self):
#         config = parse_sql_code_to_config(
#             """
#             -- SPETLR.CONFIGURATOR key: MyDbAlias
#             CREATE DATABASE IF NOT EXISTS my_db_name
#             COMMENT "Really great db"
#             LOCATION "/somewhere/over/the/rainbow";
#             """
#         )

#         k, v = config.popitem()
#         self.assertEqual(k, "MyDbAlias")
#         c = Configurator()
#         c.register(k, v)
#         db = DeltaDatabaseSpec.from_tc(k)
#         self.assertEqual(
#             db,
#             DeltaDatabaseSpec(
#                 name="my_db_name",
#                 location="/somewhere/over/the/rainbow",
#                 comment="Really great db",
#             ),
#         )

#     def test_DeltaDatabaseSpec_to_sql(self):
#         db = DeltaDatabaseSpec(
#             name="my_db_name",
#             location="/somewhere/over/the/rainbow",
#             comment="Really great db",
#         )
#         self.assertEqual(
#             db.get_create_sql(),
#             (
#                 "CREATE SCHEMA IF NOT EXISTS my_db_name\n"
#                 '  COMMENT "Really great db"\n'
#                 '  LOCATION "dbfs:/somewhere/over/the/rainbow"'
#             ),
#         )

#     def test_DeltaDatabaseSpec_repr(self):
#         db = DeltaDatabaseSpec(
#             name="my_db_name",
#             location="/somewhere/over/the/rainbow",
#             comment="Really great db",
#         )
#         as_text = repr(db)

#         self.assertEqual(db, eval(as_text))

#     def test_no_location(self):
#         self.assertEqual(
#             DeltaDatabaseSpec.from_sql(
#                 """
#             CREATE DATABASE spark_catalog.mydb
#             COMMENT "nice data"
#         """
#             ),
#             DeltaDatabaseSpec(name="spark_catalog.mydb", comment="nice data"),
#         )

#     def test_only_name(self):
#         self.assertEqual(
#             DeltaDatabaseSpec.from_sql("CREATE DATABASE mydb"),
#             DeltaDatabaseSpec(name="mydb"),
#         )
