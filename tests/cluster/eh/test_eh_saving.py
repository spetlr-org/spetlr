import time
import unittest
from datetime import datetime, timedelta

from pyspark.sql import functions as f

from atc.config_master import TableConfigurator
from atc.eh import EventHubCapture
from atc.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from atc.functions import init_dbutils
from atc.spark import Spark
from tests.cluster.values import resourceName

from .AtcEh import AtcEh


class EventHubsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        TableConfigurator().clear_all_configurations()

    def test_01_publish(self):
        eh = AtcEh()

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")
        eh.save_data(
            df.select(
                f.encode(
                    f.to_json(f.struct("*")),
                    "utf-8",
                ).alias("body")
            )
        )

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
        tc = TableConfigurator()
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
        tc = TableConfigurator()
        tc.register(
            "AtcEh",
            {
                "name": "AtcEh",
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
