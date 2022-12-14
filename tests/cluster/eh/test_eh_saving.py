import time
import unittest
from datetime import datetime, timedelta, timezone

from atc_tools.time import dt_utc
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from atc import Configurator
from atc.delta import DeltaHandle
from atc.eh import EventHubCapture, EventHubJsonPublisher
from atc.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from atc.etl import Transformer
from atc.functions import init_dbutils
from atc.orchestrators import EhJsonToDeltaOrchestrator
from atc.spark import Spark
from tests.cluster.values import resourceName

from .AtcEh import AtcEh


class EventHubsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

    def test_01_publish(self):
        eh = AtcEh()

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")
        publisher = EventHubJsonPublisher(eh)
        publisher.save(df)

    def test_02_wait_for_capture_files(self):
        # wait until capture file appears
        dbutils = init_dbutils()

        limit = datetime.now() + timedelta(minutes=10)

        while datetime.now() < limit:
            conts = {
                item.name for item in dbutils.fs.ls(f"/mnt/{resourceName()}/silver")
            }
            if f"{resourceName()}/" in conts:
                break
            else:
                time.sleep(10)
                continue
        else:
            self.assertTrue(False, "The capture file never appeared.")

        self.assertTrue(True, "The capture file has appeared.")

    def test_03_read_eh_capture(self):
        tc = Configurator()
        tc.register(
            "AtcEh",
            {
                "name": "AtcEh",
                "path": f"/mnt/{resourceName()}/silver/{resourceName()}/atceh",
                "format": "avro",
                "partitioning": "ymd",
            },
        )
        eh = EventHubCapture.from_tc("AtcEh")
        df = eh.read()

        df = df.select(f.from_json("body", "id int, name string").alias("body")).select(
            "body.*"
        )
        rows = {tuple(row) for row in df.collect()}
        self.assertEqual({(1, "a"), (2, "b")}, rows)

    def test_04_read_eh_capture_extractor(self):
        tc = Configurator()
        tc.register(
            "AtcEh",
            {
                "path": f"/mnt/{resourceName()}/silver/{resourceName()}/atceh",
                "format": "avro",
                "partitioning": "ymd",
            },
        )
        eh = EventHubCaptureExtractor.from_tc("AtcEh")
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

    def test_05_eh_json_orchestrator(self):
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

        eh_orch = EhJsonToDeltaOrchestrator.from_tc("AtcEh", "CpTblYMD")
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

        eh_orch2 = EhJsonToDeltaOrchestrator.from_tc("AtcEh", "CpTblDate")
        eh_orch2.filter_with(IdFilter())
        eh_orch2.execute()

        df2 = DeltaHandle.from_tc("CpTblDate").read().select("id", "name")

        rows = {tuple(row) for row in df2.collect()}
        self.assertEqual({(2, "b")}, rows)
