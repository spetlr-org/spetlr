import unittest

from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark
from spetlr.testutils.stop_test_streams import stop_test_streams


@unittest.skip("TODO: Test uses mount points")
class TestStopTestStreamsOnCluster(DataframeTestCase):
    """
    Tests if a test stream is stopped.

    """

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()

    def test_01_stop_test(self):
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )

        db = DbHandle.from_tc("MyDb")
        db.create()

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/_checkpoint_path_tbl",
                "query_name": "testquerytbl{ID}",
            },
        )

        mirror_cp_path = "/mnt/spetlr/silver/testdb{ID}/_checkpoint_path_tblmirror"
        tc.register(
            "MyTblMirror",
            {
                "name": "TestDb{ID}.TestTblMirror",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtblmirror",
                "format": "delta",
                "checkpoint_path": mirror_cp_path,
                "await_termination": True,
                "query_name": "testquerytblmirror{ID}",
            },
        )

        dh = DeltaHandle.from_tc("MyTbl")
        Spark.get().sql(
            f"""
                CREATE TABLE {dh.get_tablename()}
                (
                id int,
                name string
                )"""
        )

        dh_mirror = DeltaHandle.from_tc("MyTblMirror")
        Spark.get().sql(
            f"""
                CREATE TABLE {dh_mirror.get_tablename()}
                (
                id int,
                name string
                )"""
        )
        _query_name = Configurator().get("MyTblMirror", "query_name")

        (
            Spark.get()
            .readStream.table(dh.get_tablename())
            .writeStream.trigger(processingTime="2 seconds")
            .queryName(_query_name)
            .option(
                "checkpointLocation",
                Configurator().get("MyTblMirror", "checkpoint_path"),
            )
            .toTable(dh_mirror.get_tablename())
        )

        self.assert_stream_exists(_query_name)

        stop_test_streams()

        self.assert_stream_not_exists(_query_name)

    def assert_stream_exists(self, stream_name: str) -> None:
        _test_stream_exists = False
        for stream in Spark.get().streams.active:
            if stream.name == stream_name:
                _test_stream_exists = True

        self.assertTrue(_test_stream_exists)

    def assert_stream_not_exists(self, stream_name: str) -> None:
        _test_stream_exists = False
        for stream in Spark.get().streams.active:
            if stream.name == stream_name:
                _test_stream_exists = True

        self.assertFalse(_test_stream_exists)
