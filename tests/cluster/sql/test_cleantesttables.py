import unittest

from spetlr import Configurator
from spetlr.utils.SqlCleanupSingleTestTables import SqlCleanupSingleTestTables

from . import extras
from .DeliverySqlExecutor import DeliverySqlExecutor
from .DeliverySqlServer import DeliverySqlServer


class SqlCleanTablesTests(unittest.TestCase):
    tc = None
    sql_server = None

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()

        # Register the delivery table for the table configurator
        cls.tc = Configurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

        # Ensure no table is there
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    @classmethod
    def tearDownClass(cls):
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    def test_01_remove_only_current_UUID_tables(self):
        # Create tables and save table names
        DeliverySqlExecutor().execute_sql_file("test1")
        old_table_name_1 = self.tc.table_name("SqlTestTable1")
        old_table_name_2 = self.tc.table_name("SqlTestTable2")

        # Create a new UUID
        self.tc.set_debug()

        DeliverySqlExecutor().execute_sql_file("test1")
        new_table_name_1 = self.tc.table_name("SqlTestTable1")
        new_table_name_2 = self.tc.table_name("SqlTestTable2")

        # Cleanup new tables
        SqlCleanupSingleTestTables(server=self.sql_server).execute()

        # The old tables should still exist
        self.assert_sql_table_exists(old_table_name_1)
        self.assert_sql_table_exists(old_table_name_2)

        # The new tables should be dropped
        self.assert_sql_table_not_exists(new_table_name_1)
        self.assert_sql_table_not_exists(new_table_name_2)

    def test_02_remove_all_test_tables(self):
        pass

    def assert_sql_table_not_exists(self, table_name):
        results = table_name

        self.assertEqual(results.count(), 0)

    def assert_sql_table_exists(self, table_name):
        results = table_name

        self.assertEqual(results.count(), 1)

    def assert_sql_exists_helper(self, table_name):
        sql_argument = f"""
                               (SELECT * FROM INFORMATION_SCHEMA.TABLES
                               WHERE TABLE_NAME = '{table_name}') target
                               """
        table_exists = self.sql_server.load_sql(sql_argument)

        return table_exists
