import unittest

from pyspark.sql import functions as f

from atc.spark import Spark

from .AtcEh import AtcEh


class EventHubsTests(unittest.TestCase):
    def test_publish(self):
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
