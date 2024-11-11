## Commenting out as these tests are not working as expected in UC-enabled clusters.


# import unittest

# from spetlr import Configurator
# from spetlr.configurator.sql.parse_sql import parse_sql_code_to_config
# from spetlr.deltaspec.DeltaDatabaseSpec import DeltaDatabaseSpec
# from spetlr.spark import Spark


# class TestDbSpec(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls):
#         c = Configurator()
#         c.clear_all_configurations()
#         c.set_debug()

#     def test_DbSpecFromDisk(self):
#         c = Configurator()
#         master_sql = """
#             -- SPETLR.CONFIGURATOR key: MyDbAlias
#             CREATE DATABASE IF NOT EXISTS my_db_name{ID}
#             COMMENT "Really great db"
#             LOCATION "/tmp/my_db{ID}";
#             """
#         config = parse_sql_code_to_config(master_sql)

#         k, v = config.popitem()
#         self.assertEqual(k, "MyDbAlias")
#         c.register(k, v)
#         db_parsed = DeltaDatabaseSpec.from_tc(k)
#         Spark.get().sql(master_sql.format(**c.get_all_details()))
#         db_read = DeltaDatabaseSpec.from_spark(db_parsed.fully_substituted().name)

#         self.assertEqual(db_parsed.fully_substituted(), db_read)

#         Spark.get().sql(f"DROP DATABASE {db_parsed.fully_substituted().name} CASCADE")
