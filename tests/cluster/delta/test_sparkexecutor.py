import unittest

from atc.config_master import TableConfigurator
from atc.delta import DbHandle, DeltaHandle
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class DeliverySparkExecutorTests(unittest.TestCase):
    dh = None
    tc = None
    dbh = None

    @classmethod
    def setUpClass(cls):

        # Register the delivery table for the table configurator
        cls.tc = TableConfigurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

        cls.dbh = DbHandle
        cls.dh = DeltaHandle

        # Ensure no table is there
        cls.dbh.from_tc("SparkTestDb").drop()

        cls.dh.from_tc("SparkTestTable1").drop()

    @classmethod
    def tearDownClass(cls):
        cls.dbh.from_tc("SparkTestDb").drop()
        cls.dh.from_tc("SparkTestTable1").drop()

    def test_can_execute(self):
        SparkSqlExecutor().execute_sql_file("test1")

        self.dh.from_tc("SparkTestTable1").read()

        self.assertTrue(True)
