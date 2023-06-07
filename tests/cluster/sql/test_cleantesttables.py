import unittest

from spetlr import Configurator
from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.utils.SqlCleanupSingleTestTables import SqlCleanupSingleTestTables

from . import extras
from .DeliverySqlExecutor import DeliverySqlExecutor
from .DeliverySqlServer import DeliverySqlServer


class SqlCleanTablesTests(unittest.TestCase):
    """
    This tests simulates the concurrent execution of two tests:

    Test A:
    SQL Server tables is created with SPETLR Configurator debug IDs.

    Test B:
    SQL server tables is created with ANOTHER SPETLR Configurator debug ID.
    The SqlCleanupSingleTestTables is executed in the tearDownClass.

    Goal:
    Test A tables should not be affected.

    NB: This test utilizes the Configurator in a non-trivial manner.
        It is not recommended to replicate this test or Configurator setup.
    """

    tc = None
    sql_server = None

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()

        # Register the delivery table for the table configurator
        cls.setup_conf()

        # Ensure no table is there
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    @classmethod
    def tearDownClass(cls):
        cls.sql_server.drop_table("SqlTestTable1")
        cls.sql_server.drop_table("SqlTestTable2")

    def test_01_remove_only_current_UUID_tables(self):
        """
        Test remove only the current UUID tables.

        - Creates tables and saves table names.
        - Verifies that tables are readable.
        - Generates a new UUID and recreates the tables.
        - Verifies that the new tables are readable.
        - Cleans up the new tables.
        - Verifies that the old tables still exist and the new tables are removed.
        """
        # Create tables and save table names
        DeliverySqlExecutor().execute_sql_file("test1")
        old_table_name_1 = self.tc.table_name("SqlTestTable1")
        old_table_name_2 = self.tc.table_name("SqlTestTable2")

        # Should be able to read tables here
        self.assert_sql_table_exists(old_table_name_1)
        self.assert_sql_table_exists(old_table_name_2)

        # Create a new UUID
        self.tc.regenerate_unique_id_and_clear_conf()
        self.setup_conf()

        DeliverySqlExecutor().execute_sql_file("test1")
        new_table_name_1 = self.tc.table_name("SqlTestTable1")
        new_table_name_2 = self.tc.table_name("SqlTestTable2")

        # Should be able to read new tables here
        self.assert_sql_table_exists(new_table_name_1)
        self.assert_sql_table_exists(new_table_name_2)

        # Cleanup new tables
        SqlCleanupSingleTestTables(server=self.sql_server).execute()

        # The old tables should still exist
        self.assert_sql_table_exists(old_table_name_1)
        self.assert_sql_table_exists(old_table_name_2)

        # The new tables should be dropped

        # ## This should make the test fail.. but it doesnt ##
        self.sql_server.read_table_by_name(new_table_name_1)
        # ###################################################

        self.assert_sql_table_not_exists(new_table_name_1)
        self.assert_sql_table_not_exists(new_table_name_2)

    def test_02_if_prod_raise_error(self):
        """
        If the Configurator is in production mode,
        then an assertion error should be raised.
        """
        self.tc.set_prod()
        with self.assertRaises(OnlyUseInSpetlrDebugMode):
            SqlCleanupSingleTestTables(server=self.sql_server).execute()

    @unittest.skip
    def test_03_remove_all_test_tables(self):
        """
        Todo: Introduce a test of the SqlCleanupTestTables()
        """
        pass

    def assert_sql_table_not_exists(self, table_name):
        results = self.assert_sql_exists_helper(table_name)

        self.assertEqual(results.count(), 0)

    def assert_sql_table_exists(self, table_name):
        results = self.assert_sql_exists_helper(table_name)

        self.assertEqual(results.count(), 1)

    def assert_sql_exists_helper(self, table_name):
        """

        Table name should not have a schema like .dbo

        """

        table_name = table_name.replace("dbo.", "")

        sql_argument = f"""
                               (SELECT * FROM INFORMATION_SCHEMA.TABLES
                               WHERE TABLE_NAME = '{table_name}') target
                               """
        table_exists = self.sql_server.load_sql(sql_argument)

        return table_exists

    @classmethod
    def setup_conf(cls):
        cls.tc = Configurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()
