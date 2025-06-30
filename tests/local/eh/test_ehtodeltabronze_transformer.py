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
                -1962172208983150762,  # EventhubRowId
                -1870905906739491953,  # BodyId
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

    def test_02_eventhubrowid_unique_for_same_timestamp(self):
        # Same test‐harness setup as test_01
        test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema, self._target_cols, []
            )
        )

        # Two different JSON bodies, identical EnqueuedTimestamp & pdate
        ts = dt_utc(2021, 10, 31, 0, 0, 0)
        df_in = Spark.get().createDataFrame(
            [
                (
                    10001,  # SequenceNumber
                    "OffsetA",  # Offset
                    "SystemPropertiesA",  # SystemProperties
                    "PropertiesA",  # Properties
                    json.dumps({"id": "1"}).encode("utf-8"),  # Body
                    ts,  # pdate
                    ts,  # EnqueuedTimestamp
                ),
                (
                    10002,
                    "OffsetB",
                    "SystemPropertiesB",
                    "PropertiesB",
                    json.dumps({"id": "2"}).encode("utf-8"),
                    ts,
                    ts,
                ),
            ],
            self._capture_eventhub_output_schema,
        )

        # Run the transformer
        df_result = EhToDeltaBronzeTransformer(test_handle).process(df_in)

        # Collect the generated IDs
        ids = [row["EventhubRowId"] for row in df_result.collect()]

        # Assert we have exactly two rows, and their IDs are distinct
        self.assertEqual(len(ids), 2)
        self.assertEqual(
            len(set(ids)), 2, f"Expected 2 unique EventhubRowIds, got {ids!r}"
        )

    def test_03_eventhubrowid_unique_for_same_body_different_timestamp(self):
        # Same test‐harness setup as test_01
        test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self._target_schema, self._target_cols, []
            )
        )

        # One JSON body, two different EnqueuedTimestamps & pdates
        body_bytes = json.dumps({"id": "1234", "name": "John"}).encode("utf-8")
        ts1 = dt_utc(2021, 10, 31, 0, 0, 0)
        ts2 = dt_utc(2021, 10, 31, 0, 0, 1)  # 1 second later

        df_in = Spark.get().createDataFrame(
            [
                (
                    30001,  # SequenceNumber
                    "OffsetA",  # Offset
                    "SysPropsA",  # SystemProperties
                    "PropsA",  # Properties
                    body_bytes,  # Body
                    ts1,  # pdate
                    ts1,  # EnqueuedTimestamp
                ),
                (
                    30002,
                    "OffsetB",
                    "SysPropsB",
                    "PropsB",
                    body_bytes,
                    ts2,
                    ts2,
                ),
            ],
            self._capture_eventhub_output_schema,
        )

        # Run the transformer
        df_result = EhToDeltaBronzeTransformer(test_handle).process(df_in)

        # Collect the generated IDs
        ids = [row["EventhubRowId"] for row in df_result.collect()]

        # Assert we have exactly two rows, and their IDs are distinct
        self.assertEqual(len(ids), 2)
        self.assertEqual(
            len(set(ids)),
            2,
            f"Expected 2 unique EventhubRowIds for same body/different timestamp, got {ids!r}",
        )
