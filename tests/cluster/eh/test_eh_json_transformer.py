import json
import unittest
from unittest.mock import Mock

from atc_tools.testing import DataframeTestCase
from atc_tools.time import dt_utc

from atc import Configurator
from atc.delta import DeltaHandle
from atc.eh import EventHubCaptureExtractor
from atc.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import EhJsonToDeltaExtractor
from atc.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)
from atc.spark import Spark


class JsonEhTransformerUnitTests(DataframeTestCase):
    tc: Configurator

    @classmethod
    def setUpClass(cls) -> None:
        cls.tc = Configurator()
        cls.tc.set_debug()
        cls.tc.clear_all_configurations()

        cls.tc.register("TblPdate1", {"name": "TablePdate1{ID}"})
        cls.tc.register("TblPdate2", {"name": "TablePdate2{ID}"})

        spark = Spark.get()

        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate1')}")
        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate2')}")

        spark.sql(
            f"""
            CREATE TABLE {cls.tc.table_name('TblPdate1')}
            (id int, name string, BodyJson string, pdate timestamp)
            PARTITIONED BY (pdate)
        """
        )

        spark.sql(
            f"""
                    CREATE TABLE {cls.tc.table_name('TblPdate2')}
                    (id int, name string, pdate timestamp)
                    PARTITIONED BY (pdate)
                """
        )

    def test_transformer_w_body(self):
        dh = DeltaHandle.from_tc("TblPdate1")

        df_in = Spark.get().createDataFrame(
            [
                json.dumps(
                    {
                        "id": "1234",
                        "name": "John",
                    }
                ).encode("utf-8"),
                dt_utc(2021, 10, 31, 0, 0, 0).date,  # pdate
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
            ],
            dh.read().schema,
        )

        expected = Spark.get().createDataFrame(
            [
                (
                    1234,
                    "John",
                    json.dumps(
                        {
                            "id": "1234",
                            "name": "John",
                        }
                    ),
                    dt_utc(2021, 10, 31, 0, 0, 0).date,
                ),
            ],
            dh.read().schema,
        )

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, expected)

    def test_transformer(self):
        dh = DeltaHandle.from_tc("TblPdate2")

        df_in = Spark.get().createDataFrame(
            [
                json.dumps(
                    {
                        "id": "1234",
                        "name": "John",
                    }
                ).encode("utf-8"),
                dt_utc(2021, 10, 31, 0, 0, 0).date,  # pdate
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
            ],
            dh.read().schema,
        )

        expected = Spark.get().createDataFrame(
            [
                (
                    1234,
                    "John",
                    dt_utc(2021, 10, 31, 0, 0, 0).date,
                ),
            ],
            dh.read().schema,
        )

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, expected)
