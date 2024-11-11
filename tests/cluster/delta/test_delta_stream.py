import time
import unittest

from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.spark import Spark
from spetlr.testutils.stop_test_streams import stop_test_streams


class DeltaStreamTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()

        stop_test_streams()

    def test_01_configure(self):
        tc = Configurator()
        tc.register("MyDb", {"name": "TestDb{ID}"})

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "format": "delta",
                "checkpoint_path": "/tmp/stream{ID}/_checkpoint_path_tbl",
                "query_name": "testquerytbl{ID}",
            },
        )

        mirror_cp_path = "/tmp/stream{ID}/_checkpoint_path_tblmirror"
        tc.register(
            "MyTblMirror",
            {
                "name": "TestDb{ID}.TestTblMirror",
                "format": "delta",
                "checkpoint_path": mirror_cp_path,
                "await_termination": True,
                "query_name": "testquerytblmirror{ID}",
            },
        )

        tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
                "format": "delta",
                "checkpoint_path": "/tmp/stream{ID}/_checkpoint_path_tbl2",
                "query_name": "testquerytbl2{ID}",
            },
        )

        tc.register(
            "MyTbl4",
            {
                "name": "TestDb{ID}.TestTbl4",
                "format": "delta",
                "checkpoint_path": "/tmp/stream{ID}/_checkpoint_path_tbl4",
                "query_name": "testquerytbl4{ID}",
            },
        )

        tc.register(
            "MyTbl5",
            {
                "name": "TestDb{ID}.TestTbl5",
                "format": "delta",
                "checkpoint_path": "/tmp/stream{ID}/_checkpoint_path_tbl5",
                "query_name": "testquerytbl5{ID}",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        DeltaHandle.from_tc("MyTbl")
        DeltaHandle.from_tc("MyTblMirror")
        DeltaHandle.from_tc("MyTbl2")
        DeltaHandle.from_tc("MyTbl4")
        DeltaHandle.from_tc("MyTbl5")

    def test_02_write_data_with_deltahandle(self):
        self._overwrite_two_rows_to_table("MyTbl")

    def test_03_create(self):
        db = DbHandle.from_tc("MyDb")
        db.create()

        dh = DeltaHandle.from_tc("MyTbl")
        dh.create_hive_table()

        # test hive access:
        df = dh.read()
        self.assertEqual(2, df.count())

    def test_04_read(self):
        df = DeltaHandle.from_tc("MyTbl").read_stream()
        self.assertTrue(df.isStreaming)

    def test_05_truncate(self):
        dsh = DeltaHandle.from_tc("MyTbl")
        dsh.truncate()

        result = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(0, result.count())

    def test_06_etl(self):
        self._overwrite_two_rows_to_table("MyTbl")
        self._create_tbl_mirror()

        dh = DeltaHandle.from_tc("MyTbl")
        dh_target = DeltaHandle.from_tc("MyTblMirror")

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh, dataset_key="MyTbl"))
        o.load_into(
            StreamLoader(
                loader=SimpleLoader(dh_target, mode="append"),
                await_termination=True,
                checkpoint_path=Configurator().get("MyTblMirror", "checkpoint_path"),
                query_name=Configurator().get("MyTblMirror", "query_name"),
            )
        )
        o.execute()

        result = DeltaHandle.from_tc("MyTblMirror").read()
        self.assertEqual(2, result.count())

    def test_07_write_path_only(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        dh1 = DeltaHandle.from_tc("MyTbl")

        dh2 = DeltaHandle.from_tc("MyTbl2")

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh1, dataset_key="MyTbl"))
        o.load_into(
            StreamLoader(
                loader=SimpleLoader(dh2, mode="append"),
                await_termination=True,
                checkpoint_path=Configurator().get("MyTbl2", "checkpoint_path"),
                query_name=Configurator().get("MyTbl2", "query_name"),
            ),
        )
        o.execute()

        # Read data from mytbl2
        result = dh2.read()
        self.assertEqual(2, result.count())

    def test_09_trigger_once(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        dh1 = DeltaHandle.from_tc("MyTbl")

        dh2 = DeltaHandle.from_tc("MyTbl2")

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh1, dataset_key="MyTbl"))
        o.load_into(
            StreamLoader(
                loader=SimpleLoader(dh2, mode="append"),
                await_termination=False,
                checkpoint_path=Configurator().get("MyTbl2", "checkpoint_path"),
                trigger_type="once",
                query_name=Configurator().get("MyTbl2", "query_name"),
            ),
        )
        o.execute()

        # wait 20 sec for the stream to start
        time.sleep(20)

        stop_test_streams()

    def test_09_trigger_processing_time(self):
        self._overwrite_two_rows_to_table("MyTbl")
        # check that we can write to the table with no "name" property
        dh1 = DeltaHandle.from_tc("MyTbl")

        dh2 = DeltaHandle.from_tc("MyTbl2")

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh1, dataset_key="MyTbl"))
        o.load_into(
            StreamLoader(
                loader=SimpleLoader(dh2, mode="append"),
                options_dict={},
                await_termination=False,
                checkpoint_path=Configurator().get("MyTbl2", "checkpoint_path"),
                trigger_type="processingtime",
                trigger_time_seconds=5,
                query_name=Configurator().get("MyTbl2", "query_name"),
            ),
        )
        o.execute()

        # wait 10 sec for the stream to start
        time.sleep(10)

        stop_test_streams()

    def test_10_delete(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh.drop_and_delete()
        with self.assertRaises(AnalysisException):
            dh.read()

    def _overwrite_two_rows_to_table(self, tblid: str):
        dh = DeltaHandle.from_tc(tblid)

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")

        dh.overwrite(df, mergeSchema=True)

    def _create_tbl_mirror(self):
        dh = DeltaHandle.from_tc("MyTblMirror")
        Spark.get().sql(
            f"""
                            CREATE TABLE {dh.get_tablename()}
                            (
                            id int,
                            name string
                            )
                        """
        )
