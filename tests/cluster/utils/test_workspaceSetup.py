from unittest import TestCase

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark


class TestStopTestStreamsOnCluster(TestCase):
    """
    Verify expected setup

    """

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()

    def test_01_stop_test(self):
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()
        tc.register("MyDb", {"name": "TestDb{ID}"})

        db = DbHandle.from_tc("MyDb")
        db.create()

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
            },
        )
        tbl = DeltaHandle.from_tc("MyTbl")
        tbl.create_hive_table()

        spark = Spark.get()
        catalog = spark.catalog.currentCatalog()
        print("Current catalog is", catalog)
        print(spark.sql(f"DESCRIBE DETAIL {tbl.get_tablename()}").collect()[0].asDict())
