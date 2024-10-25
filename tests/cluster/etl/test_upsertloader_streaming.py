import unittest
from typing import List, Tuple

from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.upsert_loader_streaming import UpsertLoaderStreaming
from spetlr.spark import Spark
from spetlr.testutils.stop_test_streams import stop_test_streams
from spetlr.utils import DataframeCreator
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


@unittest.skipUnless(
    Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4,
    f"UpsertLoader for streaming not available for Spark version {Spark.version()}",
)
class UpsertLoaderTestsDeltaStream(DataframeTestCase):
    target_dh: DeltaHandle
    target_id = "UpsertLoaderStreamingTarget"
    source_id = "UpsertLoaderStreamingSource"

    join_cols = ["col1", "col2"]

    data1 = [
        (5, 6, "foo"),
        (7, 8, "bar"),
    ]
    data2 = [
        (1, 2, "baz"),
    ]
    data3 = [(5, 6, "boo"), (5, 7, "spam")]
    # data5 is the merge result of data2 + data3 + data4
    data4 = [(1, 2, "baz"), (5, 6, "boo"), (5, 7, "spam"), (7, 8, "bar")]

    dummy_columns: List[str] = ["col1", "col2", "col3"]

    @classmethod
    def setUpClass(cls) -> None:
        tc = Configurator()
        tc.add_resource_path(extras)
        tc.set_debug()

        tc.register(
            "MyStream",
            {
                "query_name": "testquerytbl{ID}",
            },
        )

        SparkSqlExecutor().execute_sql_file("upsertloader-test")

        cls.source_dh = DeltaHandle.from_tc(cls.source_id)
        cls.target_dh = DeltaHandle.from_tc(cls.target_id)

        cls.dummy_schema = cls.target_dh.read().schema

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("UpsertLoaderDb").drop_cascade()

        stop_test_streams()

    def test_01_can_perform_incremental_on_empty(self):
        """Stream two rows to the empty target table"""

        self._create_test_source_data(data=self.data1)

        self.execute_upsert_stream_orchestrator()

        self.assertDataframeMatches(self.target_dh.read(), None, self.data1)

    def test_02_can_perform_incremental_append(self):
        """The target table is already filled from before.
        One new rows appear in the source table to be streamed
        """

        existing_rows = self.target_dh.read().collect()
        self.assertEqual(2, len(existing_rows))

        self._create_test_source_data(data=self.data2)

        self.execute_upsert_stream_orchestrator()

        self.assertDataframeMatches(
            self.target_dh.read(), None, self.data1 + self.data2
        )

    def test_03_can_perform_merge(self):
        """The target table is already filled from before.
        Two new rows appear in the source table - one of them will be merged.
        """
        existing_rows = self.target_dh.read().collect()
        self.assertEqual(3, len(existing_rows))

        self._create_test_source_data(data=self.data3)

        self.execute_upsert_stream_orchestrator()

        self.assertDataframeMatches(self.target_dh.read(), None, self.data4)

    def _create_test_source_data(
        self, tableid: str = None, data: List[Tuple[int, int, str]] = None
    ):
        if tableid is None:
            tableid = self.source_id
        if data is None:
            raise ValueError("Testdata missing.")

        dh = DeltaHandle.from_tc(tableid)

        Spark.get().sql(
            f"""
            CREATE TABLE IF NOT EXISTS {dh.get_tablename()}
            (
            id int,
            name string
            )
            """
        )

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, data
        )

        dh.append(df_source)

    def execute_upsert_stream_orchestrator(self):
        o = Orchestrator()
        o.extract_from(StreamExtractor(self.source_dh, dataset_key="MyTbl"))
        o.load_into(
            UpsertLoaderStreaming(
                handle=self.target_dh,
                options_dict={},
                trigger_type="availablenow",
                checkpoint_path=Configurator().get(self.target_id, "checkpoint_path"),
                await_termination=True,
                upsert_join_cols=self.join_cols,
                query_name=Configurator().get("MyStream", "query_name"),
            )
        )
        o.execute()
