import time
import unittest
from datetime import datetime, timedelta

from pyspark.sql import functions as f

from atc.config_master import TableConfigurator
from atc.eh import EventHubCapture
from atc.functions import init_dbutils
from atc.spark import Spark

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

        limit = datetime.now() + timedelta(minutes=5)
        while datetime.now() < limit:
            conts = {item.name for item in dbutils.fs.ls("/mnt/githubatc/silver")}
            if "githubatc/" in conts:
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
                "path": "/mnt/githubatc/silver/githubatc/atceh",
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
