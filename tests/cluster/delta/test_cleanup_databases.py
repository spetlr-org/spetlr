import unittest

from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark
from spetlr.testutils.CleanupTestDatabases import CleanupTestDatabases
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class CleanTestTablesTests(unittest.TestCase):
    """
    This tests simulates the concurrent execution of two tests:

    Test A:
    Spark SQL databases is created with SPETLR Configurator debug IDs.

    Test B:
    Spark SQL databases is created with ANOTHER SPETLR Configurator debug ID.
    The CleanTestDatabases is executed in the tearDownClass.

    Goal:
    Test A databases should not be affected.

    NB: This test utilizes the Configurator in a non-trivial manner.
        It is not recommended to replicate this test or Configurator setup.
    """

    dh = None
    tc = None
    dbh = None

    @classmethod
    def setUpClass(cls):
        # Register the delivery table for the table configurator
        cls.setup_conf()

        cls.dbh = DbHandle
        cls.dh = DeltaHandle

        # Ensure no table is there
        cls.dbh.from_tc("SparkTestDb").drop_cascade()
        cls.dh.from_tc("SparkTestTable1").drop()
        cls.dbh.from_tc("SparkTestDb2").drop_cascade()
        cls.dh.from_tc("SparkTestTable2").drop()

    @classmethod
    def tearDownClass(cls):
        cls.dbh.from_tc("SparkTestDb").drop_cascade()
        cls.dh.from_tc("SparkTestTable1").drop()
        cls.dbh.from_tc("SparkTestDb2").drop_cascade()
        cls.dh.from_tc("SparkTestTable2").drop()

    def test_01_remove_only_current_UUID_tables(self):
        """
        Test remove only the current UUID databases.

        - Creates databases and saves table names.
        - Verifies that tables are readable.
        - Generates a new UUID and recreates the tables (and databases).
        - Verifies that the new tables are readable.
        - Cleans up the new databases.
        - Verifies that the old tables still exist and the new tables are removed.
        """
        # Create tables and save table names
        SparkSqlExecutor().execute_sql_file("test1")
        SparkSqlExecutor().execute_sql_file("test2_debug")
        old_table_name_1 = self.tc.table_name("SparkTestTable1")
        old_table_name_2 = self.tc.table_name("SparkTestTable2")

        # Should be able to read tables here
        old_df_1 = Spark.get().table(old_table_name_1)
        old_df_2 = Spark.get().table(old_table_name_2)

        self.assertEqual(old_df_1.count(), 0)
        self.assertEqual(old_df_2.count(), 0)

        # Create a new UUID
        self.tc.regenerate_unique_id_and_clear_conf()
        self.setup_conf()

        SparkSqlExecutor().execute_sql_file("test1")
        SparkSqlExecutor().execute_sql_file("test2_debug")
        new_table_name_1 = self.tc.table_name("SparkTestTable1")
        new_table_name_2 = self.tc.table_name("SparkTestTable2")

        # Should be able to read tables here
        new_df_1 = Spark.get().table(new_table_name_1)
        new_df_2 = Spark.get().table(new_table_name_2)

        self.assertEqual(new_df_1.count(), 0)
        self.assertEqual(new_df_2.count(), 0)

        # Cleanup new tables
        CleanupTestDatabases()

        # The old tables should still exist
        old_df_1 = Spark.get().table(old_table_name_1)
        old_df_2 = Spark.get().table(old_table_name_2)

        self.assertEqual(old_df_1.count(), 0)
        self.assertEqual(old_df_2.count(), 0)

        # The new tables should be removed
        with self.assertRaises(AnalysisException):
            Spark.get().table(new_table_name_1)

        with self.assertRaises(AnalysisException):
            Spark.get().table(new_table_name_2)

    @classmethod
    def setup_conf(cls):
        cls.tc = Configurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()
