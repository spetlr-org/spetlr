import unittest
from unittest.mock import Mock

from atc_tools.time import dt_utc

from atc.config_master import TableConfigurator
from atc.delta import DeltaHandle
from atc.eh import EventHubCaptureExtractor
from atc.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import EhJsonToDeltaExtractor
from atc.spark import Spark


class JsonEhOrchestratorUnitTests(unittest.TestCase):
    tc: TableConfigurator

    @classmethod
    def setUpClass(cls) -> None:
        cls.tc = TableConfigurator()
        cls.tc.clear_all_configurations()
        cls.tc.register("TblYMD", {"name": "TableYMD"})
        cls.tc.register("TblYMDH", {"name": "TableYMDH"})
        cls.tc.register("TblPdate", {"name": "TablePdate"})

        spark = Spark.get()
        spark.sql("DROP TABLE IF EXISTS TableYMD")
        spark.sql("DROP TABLE IF EXISTS TableYMDH")
        spark.sql("DROP TABLE IF EXISTS TablePdate")

        spark.sql(
            """
            CREATE TABLE TableYMD
            (id int, name string, y int, m int, d int)
            PARTITIONED BY (y,m,d)
        """
        )
        spark.sql(
            """
            CREATE TABLE TableYMDH
            (id int, name string, y int, m int, d int, h int)
            PARTITIONED BY (y,m,d,h)
        """
        )
        spark.sql(
            """
            CREATE TABLE TablePdate
            (id int, name string, pdate timestamp)
            PARTITIONED BY (pdate)
        """
        )

    def test_reading_YMD(self):
        dh = DeltaHandle.from_tc("TblYMD")
        dh.truncate()
        dh.append(
            Spark.get().createDataFrame(
                [
                    (42, "spam", 2019, 6, 23),
                    # this is where the eventhub should read from:
                    (84, "eggs", 2020, 9, 5),
                ],
                dh.read().schema,
            )
        )
        eh = Mock(EventHubCaptureExtractor)
        eh.get_partitioning.side_effect = lambda: ["y", "m", "d"]

        EhJsonToDeltaExtractor(dh=dh, eh=eh).read()

        # check that the truncation worked
        self.assertEqual(len(dh.read().collect()), 1)

        # check that the from_partition argument is as expected
        eh.read.assert_called_once_with(from_partition=dt_utc(2020, 9, 5))

    def test_reading_YMDH(self):
        dh = DeltaHandle.from_tc("TblYMDH")
        dh.truncate()
        dh.append(
            Spark.get().createDataFrame(
                [
                    (42, "spam", 2019, 6, 23, 15),
                    # this is where the eventhub should read from:
                    (84, "eggs", 2020, 9, 5, 12),
                ],
                dh.read().schema,
            )
        )
        eh = Mock(EventHubCaptureExtractor)
        eh.get_partitioning.side_effect = lambda: ["y", "m", "d", "h"]

        EhJsonToDeltaExtractor(dh=dh, eh=eh).read()

        # check that the truncation worked
        self.assertEqual(len(dh.read().collect()), 1)

        # check that the from_partition argument is as expected
        eh.read.assert_called_once_with(from_partition=dt_utc(2020, 9, 5, 12))

    def test_reading_Pdate(self):
        dh = DeltaHandle.from_tc("TblPdate")
        dh.truncate()
        dh.append(
            Spark.get().createDataFrame(
                [
                    (42, "spam", dt_utc(2019, 6, 23, 15)),
                    # this is where the eventhub should read from:
                    (84, "eggs", dt_utc(2020, 9, 5, 12)),
                ],
                dh.read().schema,
            )
        )
        eh = Mock(EventHubCaptureExtractor)
        eh.get_partitioning.side_effect = lambda: ["y", "m", "d", "h"]

        EhJsonToDeltaExtractor(dh=dh, eh=eh).read()

        # check that the truncation worked
        self.assertEqual(len(dh.read().collect()), 1)

        # check that the from_partition argument is as expected
        eh.read.assert_called_once_with(from_partition=dt_utc(2020, 9, 5, 12))
