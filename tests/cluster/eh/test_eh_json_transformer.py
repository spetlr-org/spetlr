import json

from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType
from spetlrtools.testing import DataframeTestCase
from spetlrtools.time import dt_utc

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)
from spetlr.spark import Spark


class JsonEhTransformerUnitTests(DataframeTestCase):
    tc: Configurator
    capture_eventhub_output_schema = StructType(
        [
            StructField("Body", BinaryType(), True),
            StructField("pdate", TimestampType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
        ]
    )

    df_in = Spark.get().createDataFrame(
        [
            (
                json.dumps(
                    {
                        "id": 1234,
                        "name": "John",
                    }
                ).encode(
                    "utf-8"
                ),  # Body
                dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
            ),
        ],
        capture_eventhub_output_schema,
    )
    case_sensitivity_supported = False

    @classmethod
    def setUpClass(cls) -> None:
        cls.tc = Configurator()
        cls.tc.clear_all_configurations()
        cls.tc.set_debug()

        cls.tc.register("TblPdate1", {"name": "TablePdate1{ID}"})
        cls.tc.register("TblPdate2", {"name": "TablePdate2{ID}"})
        cls.tc.register("TblPdate3", {"name": "TablePdate3{ID}"})
        cls.tc.register("TblPdate4", {"name": "TablePdate4{ID}"})

        spark = Spark.get()

        cls.case_sensitivity_supported = spark.version >= "3.4.1"

        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate1')}")
        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate2')}")
        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate3')}")
        spark.sql(f"DROP TABLE IF EXISTS {cls.tc.table_name('TblPdate4')}")

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate1')}
                (id int, name string, BodyJson string, pdate timestamp,
                EnqueuedTimestamp timestamp)
                PARTITIONED BY (pdate)
            """
        )

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate2')}
                (id int, name string, pdate timestamp, EnqueuedTimestamp timestamp)
                PARTITIONED BY (pdate)
            """
        )

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate3')}
                (id int, name string, pdate timestamp, EnqueuedTimestamp timestamp,
                Unknown string)
                PARTITIONED BY (pdate)
            """
        )

        spark.sql(
            f"""
                CREATE TABLE {cls.tc.table_name('TblPdate4')}
                (id int, Name string, pdate timestamp, EnqueuedTimestamp timestamp)
                PARTITIONED BY (pdate)
            """
        )

    def test_01_transformer_w_body(self):
        """Tests whether the body is saved as BodyJson"""

        dh = DeltaHandle.from_tc("TblPdate1")

        expected = [
            (
                1234,
                "John",
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
                json.dumps(
                    {
                        "id": 1234,
                        "name": "John",
                    }
                ),  # BodyJson is at the end of select statement
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)

    def test_02_transformer(self):
        """Test if the data is correctly extracted"""
        dh = DeltaHandle.from_tc("TblPdate2")

        expected = [
            (
                1234,
                "John",
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)

    def test_03_transformer_unknown_target_field(self):
        """This should test what happens if the target
        schema has a field that does not exist in the source dataframe."""
        dh = DeltaHandle.from_tc("TblPdate3")

        expected = [
            (
                1234,
                "John",
                # THe unknown column is extract as NONE here.
                # Since pdate and EnqueuedTimestamp is always added at the end.
                None,
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)

    def test_04_transformer_case_sensitive(self):
        """Test if the data is correctly extracted - case sensitive"""
        dh = DeltaHandle.from_tc("TblPdate4")

        expected = [
            (
                1234,
                None,
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        df_result = EhJsonToDeltaTransformer(target_dh=dh).process(self.df_in)

        # Check that data is correct
        self.assertDataframeMatches(df_result, None, expected)

    def test_05_transformer_case_insensitive(self):
        """Test if the data is correctly extracted - case insensitive"""
        dh = DeltaHandle.from_tc("TblPdate4")

        expected = [
            (
                1234,
                "John",
                dt_utc(2021, 10, 31, 0, 0, 0),
                dt_utc(2021, 10, 31, 0, 0, 0),
            ),
        ]

        if not self.case_sensitivity_supported:
            self.assertTrue(True)
        else:
            df_result = EhJsonToDeltaTransformer(
                target_dh=dh, case_sensitive=False
            ).process(self.df_in)

            # Check that data is correct
            self.assertDataframeMatches(df_result, None, expected)
