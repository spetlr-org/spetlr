import json
from datetime import timedelta, timezone

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

from spetlr.orchestrators.eh2bronze.EhToDeltaBronzeTransformer import (
    EhToDeltaBronzeTransformer,
)
from spetlr.spark import Spark
from spetlr.utils import DataframeCreator


class EhtoDeltaTransformerUnitTests(DataframeTestCase):
    _target_schema = StructType(
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

    _target_cols = [
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

    def test_01(self):
        test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema, self._target_cols, []
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
                            "id": "1234",
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

        expected = [
            (
                298587303917952901,  # EventhubRowId
                146072039196263699,  # BodyId
                json.dumps(  # Body (as string)
                    {
                        "id": "1234",
                        "name": "John",
                    }
                ),
                dt_utc(2021, 10, 31, 0, 0, 0),  # EnqueuedTimestamp
                22222,
                "OffsetTest",
                "SystemPropertiesTest",
                "PropertiesTest",
                dt_utc(2021, 10, 31, 0, 0, 0),  # pdate
            ),
        ]

        df_result = EhToDeltaBronzeTransformer(test_handle).process(df_in)

        # Assert on all column except streaming time
        self.assertDataframeMatches(
            df_result,
            [
                "EventhubRowId",
                "BodyId",
                "Body",
                "EnqueuedTimestamp",
                "SequenceNumber",
                "Offset",
                "SystemProperties",
                "Properties",
                "pdate",
            ],
            expected,
        )

        # The streaming is tested assuming that the time of the transformation
        # was less than 5 min ago
        test_time = dt_utc()
        self.assertLess(
            test_time
            - df_result.collect()[0]["StreamingTime"].replace(tzinfo=timezone.utc),
            timedelta(minutes=5),
        )
