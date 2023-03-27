import json
from unittest.mock import Mock, patch

from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase
from spetlrtools.testing.TestHandle import TestHandle
from spetlrtools.time import dt_utc

from spetlr.orchestrators import (
    EhToDeltaBronzeOrchestrator,
    EhToDeltaSilverOrchestrator,
)
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import (
    EhJsonToDeltaExtractor,
)
from spetlr.spark import Spark
from spetlr.utils import DataframeCreator


class EhtoBronzeAndSilverUnitTests(DataframeTestCase):
    """
    This tests both the bronze and silver eventhub orchestrator.

    """

    _target_schema_bronze = StructType(
        [
            StructField("EventhubRowId", LongType(), True),
            StructField("BodyId", LongType(), True),
            StructField("Body", StringType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
            StructField("StreamingTime", TimestampType(), True),
            StructField("SequenceNumber", LongType(), True),
            StructField("Offset", StringType(), True),
            StructField("SystemProperties", StringType(), True),
            StructField("Properties", StringType(), True),
            StructField("pdate", TimestampType(), True),
        ]
    )

    _target_cols_bronze = [
        "EventhubRowId",
        "BodyId",
        "Body",
        "EnqueuedTimestamp",
        "StreamingTime",
        "SequenceNumber",
        "Offset",
        "SystemProperties",
        "Properties",
        "pdate",
    ]

    _target_schema_silver = StructType(
        [
            StructField("Id", StringType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
            StructField("pdate", TimestampType(), True),
        ]
    )

    _target_cols_silver = ["Id", "EnqueuedTimestamp", "pdate"]

    _capture_eventhub_output_schema = StructType(
        [
            StructField("SequenceNumber", LongType(), True),
            StructField("Offset", StringType(), True),
            StructField("SystemProperties", StringType(), True),
            StructField("Properties", StringType(), True),
            StructField("Body", BinaryType(), True),
            StructField("pdate", TimestampType(), True),
            StructField("EnqueuedTimestamp", TimestampType(), True),
        ]
    )

    def test_01_test_bronze_and_silver(self):
        eh_mock = Mock()

        dh_bronze = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema_bronze, self._target_cols_bronze, []
            )
        )

        dh_silver = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema_silver, self._target_cols_silver, []
            )
        )

        df_in = Spark.get().createDataFrame(
            [
                (
                    22222,
                    "OffsetTest",
                    "SystemPropertiesTest",
                    "PropertiesTest",
                    json.dumps(
                        {
                            "Id": "1234",
                            "name": "John",
                        }
                    ).encode(
                        "utf-8"
                    ),  # Body
                    dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
                    dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
                ),
            ],
            self._capture_eventhub_output_schema,
        )

        expected_silver = [
            (
                "1234",  # Id
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
                dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
            ),
        ]

        with patch.object(EhJsonToDeltaExtractor, "read", return_value=df_in) as p1:
            bronze_orchestrator = EhToDeltaBronzeOrchestrator(eh=eh_mock, dh=dh_bronze)

            bronze_orchestrator.execute()

            dh_bronze_mock = Mock()
            dh_bronze_mock.read = Mock(return_value=dh_bronze.appended)

            silver_orchestrator = EhToDeltaSilverOrchestrator(
                dh_source=dh_bronze_mock, dh_target=dh_silver, upsert_join_cols=["id"]
            )
            silver_orchestrator.execute()

            df_result = dh_silver.upserted

            self.assertDataframeMatches(
                df_result,
                ["Id", "EnqueuedTimestamp", "pdate"],
                expected_silver,
            )
            p1.assert_called_once()
