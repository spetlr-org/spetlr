import time
import unittest
import uuid as _uuid
from datetime import datetime, timezone

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from spetlrtools.time import dt_utc

from spetlr.configurator import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.eh import EventhubHandle
from spetlr.etl import Orchestrator, Transformer
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.etl.transformers.simple_eh_handle_write_transformer import (
    SimpleEventhubHandleTransformer,
)
from spetlr.functions import init_dbutils
from spetlr.spark import Spark
from tests.cluster.values import resourceName


class EventHubHandleTests(unittest.TestCase):
    connection_str = None
    tc = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.tc = Configurator()
        cls.tc.clear_all_configurations()

        cls.tc.set_debug()

        cls.connection_str = init_dbutils().secrets.get("secrets", "EventHubConnection")

        # eventhub = "spetlreh"
        consumer_group = "$Default"

        cls.tc.register(
            "SpetlrEh",
            {
                "path": f"/mnt/{resourceName()}/silver/{resourceName()}/spetlreh",
                "format": "avro",
                "partitioning": "ymd",
                # "eh_eventhub": eventhub,
                # "eh_namespace": resourceName(),
                "eh_consumer_group": consumer_group,
            },
        )

        cls.tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )

        cls.tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/_checkpoint_path_tbl",
                "query_name": "testquerytbl{ID}",
            },
        )

        cls.tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl2",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/"
                "_checkpoint_path_tbl2",
                "query_name": "testquerytbl2{ID}",
            },
        )

        cls.tc.register(
            "EhWrite",
            {
                "checkpoint_path": "/mnt/spetlr/silver/ehwrite{ID}/"
                "_checkpoint_path_tbl",
            },
        )

        cls.UUID_test1 = _uuid.uuid4().hex

        DbHandle.from_tc("MyDb").create()

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()

    def test_01_publish_and_read(self):
        """
        Test a simple write to the eventhub

        """
        eh = EventhubHandle.from_tc("SpetlrEh", connection_str=self.connection_str)

        df = Spark.get().createDataFrame(
            [(1, self.UUID_test1), (2, self.UUID_test1)], "id int, name string"
        )

        df = SimpleEventhubHandleTransformer().process(df)

        eh.overwrite(df)

        time.sleep(100)

        df = eh.read()

        self.assertGreaterEqual(self._count_uuid_rows(df), 2)

        df = eh.read().select("enqueuedTime")
        self.assertEqual(
            df.schema.fields[0].dataType.typeName().upper(),
            "TIMESTAMP",
        )
        row_written: datetime = df.take(1)[0][0]
        self.assertLess(
            abs((row_written.astimezone(timezone.utc) - dt_utc()).total_seconds()), 1000
        )

    def test_02_test_streaming_read(self):
        """
        test_01 should have been runned first,
        this ensures data for the eventhub.

        This test then streams from the eventhub into a delta table.

        """
        eh_source = EventhubHandle.from_tc(
            "SpetlrEh", connection_str=self.connection_str
        )
        dh_target = DeltaHandle.from_tc("MyTbl")

        (
            Orchestrator()
            .extract_from(
                StreamExtractor(
                    eh_source,
                    dataset_key="SpetlrEh",
                )
            )
            .load_into(
                StreamLoader(
                    loader=SimpleLoader(handle=dh_target, mode="append"),
                    await_termination=True,
                    checkpoint_path=Configurator().get("MyTbl", "checkpoint_path"),
                )
            )
            .execute()
        )

        # Test that some of the data from the eventhub has been written
        self.assertEqual(self._count_uuid_rows(dh_target.read()), 2)

    def test_03_test_streaming_write(self):
        """
        test_02 should have been runned first.
        This ensures data for the eventhub.

        This test then streams from the delta table back to the eventhub.
        """
        eh_source = EventhubHandle.from_tc("SpetlrEh", self.connection_str)
        dh_target = DeltaHandle.from_tc("MyTbl2")

        # Add two rows to the delta table
        df = Spark.get().createDataFrame(
            [(1, self.UUID_test1), (2, self.UUID_test1)], "id int, name string"
        )
        dh_target.overwrite(df)
        self.assertGreaterEqual(dh_target.read().count(), 2)

        (
            Orchestrator()
            .extract_from(
                StreamExtractor(
                    dh_target,
                )
            )
            .transform_with(SimpleEventhubHandleTransformer())
            .load_into(
                StreamLoader(
                    loader=SimpleLoader(handle=eh_source, mode="append"),
                    await_termination=True,
                    checkpoint_path=Configurator().get("EhWrite", "checkpoint_path"),
                )
            )
            .execute()
        )

        # There should now be 4 rows in the eventhub with the unique id
        time.sleep(100)
        self.assertGreaterEqual(self._count_uuid_rows(eh_source.read()), 4)

    def _count_uuid_rows(self, df: DataFrame) -> int:
        """
        This is a helper test function
        that checks the number of rows with this test UUID.

        Since multiple tests can add data to the eventhub
        at the same time - the UUID ensures that we only work within
        the scope of this test.
        """
        df = df.select(f.col("Body").cast("string").alias("string_body"))

        return df.filter(f.col("string_body").like("%" + self.UUID_test1 + "%")).count()
