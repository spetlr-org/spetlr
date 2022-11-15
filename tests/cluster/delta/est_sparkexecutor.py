import unittest

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.spark import Spark
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class DeliverySparkExecutorTests(unittest.TestCase):
    dh = None
    tc = None
    dbh = None

    @classmethod
    def setUpClass(cls):

        # Register the delivery table for the table configurator
        cls.tc = Configurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

        cls.dbh = DbHandle
        cls.dh = DeltaHandle

        # Ensure no table is there
        cls.dbh.from_tc("SparkTestDb").drop_cascade()

        cls.dh.from_tc("SparkTestTable1").drop()

    @classmethod
    def tearDownClass(cls):
        cls.dbh.from_tc("SparkTestDb").drop_cascade()
        cls.dh.from_tc("SparkTestTable1").drop()

    def test_can_execute(self):
        SparkSqlExecutor().execute_sql_file("*", exclude_pattern="debug")

        self.dh.from_tc("SparkTestTable1").read()

        # verify that db 2 does not exist at this point
        self.assertEqual(Spark.get().sql('SHOW DATABASES LIKE "my_db2*"').count(), 0)

        SparkSqlExecutor().execute_sql_file("*")

        self.dh.from_tc("SparkTestTable2").read()
