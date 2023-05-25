import unittest

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


def delete_all_databases():
    for (db,) in Spark.get().sql("SHOW SCHEMAS").collect():
        print(f"Now deleting {db}")
        Spark.get().sql(f"DROP DATABASE {db} CASCADE")


class DeliverySparkExecutorTests(unittest.TestCase):
    dh = None
    tc = None
    dbh = None

    @classmethod
    def setUpClass(cls):
        delete_all_databases()

        # Register the delivery table for the table configurator
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        c.set_debug()

        # Ensure no table is there
        DbHandle.from_tc("SparkTestDb").drop_cascade()

        DeltaHandle.from_tc("SparkTestTable1").drop()

    @classmethod
    def tearDownClass(cls):
        DbHandle.from_tc("SparkTestDb").drop_cascade()
        DeltaHandle.from_tc("SparkTestTable1").drop()

    def test_can_execute(self):
        SparkSqlExecutor().execute_sql_file("*", exclude_pattern="debug")

        self.dh.from_tc("SparkTestTable1").read()

        # verify that db 2 does not exist at this point
        self.assertEqual(Spark.get().sql('SHOW DATABASES LIKE "my_db2*"').count(), 0)

        SparkSqlExecutor().execute_sql_file("*")

        self.dh.from_tc("SparkTestTable2").read()
