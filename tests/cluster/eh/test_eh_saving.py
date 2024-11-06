import time
import unittest
from datetime import datetime, timedelta, timezone

from pyspark.sql import DataFrame
from spetlrtools.time import dt_utc

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.eh import EventHubJsonPublisher
from spetlr.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from spetlr.etl import Transformer
from spetlr.orchestrators import EhJsonToDeltaOrchestrator
from spetlr.spark import Spark
from spetlr.utils import AzureTags

from .SpetlrEh import SpetlrEh


class EventHubsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

    def test_01_publish_and_read(self):
        eh = SpetlrEh()

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")
        publisher = EventHubJsonPublisher(eh)
        publisher.save(df)

        time.sleep(100)  # just wait the EH captures once a minute anyway.

        tc = Configurator()
        tc.register("ws", AzureTags().resource_name)

        tc.register(
            "SpetlrEh",
            {
                # This path should align with the path defined in
                # integration_databricks catalog.
                "path": "/Volumes/{ws}/volumes/capture/{ws}/spetlreh/",
                "format": "avro",
                "partitioning": "ymd",
            },
        )
        eh = EventHubCaptureExtractor.from_tc("SpetlrEh")
        df = eh.read()
        self.assertTrue(df.count(), 2)

        df = eh.read(
            (datetime.now() - timedelta(hours=24)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        )
        self.assertTrue(df.count(), 2)

        df = eh.read().select("EnqueuedTimestamp")
        self.assertEqual(
            df.schema.fields[0].dataType.typeName().upper(),
            "TIMESTAMP",
        )
        row_written: datetime = df.take(1)[0][0]
        # assert that the eventhub row was written less than 1000 seconds ago
        self.assertLess(
            abs((row_written.astimezone(timezone.utc) - dt_utc()).total_seconds()), 1000
        )

        # def test_05_eh_json_orchestrator(self):
        # the orchestrator has a complex functionality that can only be fully tested
        # on a substantial holding of capture files. That is not possible here, but
        # such tests were carried out during development.
        # The situation here only tests the basic functions.

        # Part 1, YMD partitioned
        tc = Configurator()
        tc.set_debug()
        tc.register("CpTblYMD", {"name": "CaptureTableYMD{ID}"})
        Spark.get().sql(
            f"""
            CREATE TABLE {tc.table_name('CpTblYMD')}
            (
                id int,
                name string,
                y int,
                m int,
                d int
            )
            PARTITIONED BY (y,m,d)
        """
        )

        eh_orch = EhJsonToDeltaOrchestrator.from_tc("SpetlrEh", "CpTblYMD")
        eh_orch.execute()

        df = DeltaHandle.from_tc("CpTblYMD").read().select("id", "name")

        rows = {tuple(row) for row in df.collect()}
        self.assertEqual({(1, "a"), (2, "b")}, rows)

        # Part 2, pdate partitioned.

        tc.register("CpTblDate", {"name": "CaptureTableDate{ID}"})
        Spark.get().sql(
            f"""
            CREATE TABLE {tc.table_name('CpTblDate')}
            (
                id INTEGER,
                name STRING,
                pdate TIMESTAMP
            )
            PARTITIONED BY (pdate)
        """
        )

        # test the insertion of additional filters
        class IdFilter(Transformer):
            def process(self, df: DataFrame) -> DataFrame:
                return df.filter("id>1")

        eh_orch2 = EhJsonToDeltaOrchestrator.from_tc("SpetlrEh", "CpTblDate")
        eh_orch2.filter_with(IdFilter())
        eh_orch2.execute()

        df2 = DeltaHandle.from_tc("CpTblDate").read().select("id", "name")

        rows = {tuple(row) for row in df2.collect()}
        self.assertEqual({(2, "b")}, rows)
