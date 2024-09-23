import unittest

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark
from spetlr.sql import SqlExecutor
from tests.cluster.delta import extras, views
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class DeliverySparkExecutorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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

    def test_01_can_execute(self):
        SparkSqlExecutor().execute_sql_file("*", exclude_pattern="debug")

        DeltaHandle.from_tc("SparkTestTable1").read()

        db = DbHandle.from_tc("SparkTestDb2")
        # verify that db 2 does not exist at this point
        self.assertEqual(
            Spark.get().sql(f'SHOW DATABASES LIKE "{db._name}"').count(), 0
        )

        SparkSqlExecutor().execute_sql_file("*")

        DeltaHandle.from_tc("SparkTestTable2").read()

    def test_02_can_use_replacments(self):
        e = SqlExecutor(base_module=views)
        e.execute_sql_file("*", replacements={"my_view_name": "customview"})

        _ = Spark.get().table("customview")  # excute without error
