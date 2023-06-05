import unittest

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.spark import Spark
from spetlr.sql import SqlHandle
from spetlr.utils.stop_all_streams import stop_all_streams
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


@unittest.skipUnless(
    Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4,
    f"Spetlr Streaming not available for Spark version {Spark.version()}",
)
class SqlServerStreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()
        DeliverySqlServer().drop_table("MSSQL")

        # NB: This function will interfere with active streaming
        # if tests is parallelized, consider creation a function
        # that only stops streaming set up in this class
        stop_all_streams()

    def test_01_configure(self):
        # Configure delta table
        tc = Configurator()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/_checkpoint_path_tbl",
            },
        )

        DbHandle.from_tc("MyDb").create()

        dh = DeltaHandle.from_tc("MyTbl")

        Spark.get().sql(
            f"""
               CREATE TABLE {dh.get_tablename()}
               (
               testcolumn int
               )
               LOCATION '{Configurator().get("MyTbl", "path")}'
           """
        )

        df = Spark.get().createDataFrame([(1,), (2,)], "testcolumn int")

        dh.overwrite(df, mergeSchema=True)

        # Configure sql table
        tc.register(
            "MSSQL",
            {
                "name": "dbo.stream_test{ID}",
                "checkpoint_path": "/mnt/spetlr/silver/stream_test_sql{ID}"
                "/_checkpoint_path_",
            },
        )

        _table = Configurator().table_name("MSSQL")

        sql_argument = f"""
                    IF OBJECT_ID('{_table}', 'U') IS NULL
                    BEGIN
                    CREATE TABLE {_table}
                    (
                        testcolumn INT NULL
                    )
                    END
                """

        DeliverySqlServer().execute_sql(sql_argument)

    def test_02_stream_to_sql(self):
        _table = Configurator().table_name("MSSQL")
        sql_handle = SqlHandle(_table, sql_server=DeliverySqlServer())
        dh = DeltaHandle.from_tc("MyTbl")

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh, dataset_key="MyTbl"))
        o.load_into(
            StreamLoader(
                handle=sql_handle,
                options_dict={},
                format="delta",
                await_termination=True,
                mode="append",
                checkpoint_path=Configurator().get("MSSQL", "checkpoint_path"),
            )
        )
        o.execute()

        result = sql_handle.read()
        self.assertEqual(2, result.count())
