import unittest
from datetime import datetime

import pandas as pd
from pandas.testing import assert_frame_equal

from spetlr.power_bi.SparkPandasDataFrame import SparkPandasDataFrame


class TestSparkPandasDataFrame(unittest.TestCase):
    def test_get_workspace_success(self):
        # Arrange
        json = [
            {
                "id": 3,
                "refreshType": "ViaEnhancedApi",
                "status": "Completed",
                "startTime": "2024-03-31T00:00:00Z",  # summer time: change from 1
                "endTime": "2024-03-31T02:00:00Z",  # hour to 2 hours difference
                "serviceExceptionJson": None,
                "requestId": "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                "refreshAttempts": None,
            },
            {
                "id": 2,
                "refreshType": "ViaApi",
                "status": "Completed",
                "startTime": "2024-02-26T10:00:00Z",
                "endTime": "2024-02-26T10:05:00Z",  # winter time: 1 hour
                "serviceExceptionJson": None,  # difference from UTC
                "requestId": "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                "refreshAttempts": None,
            },
            {
                "id": 1,
                "refreshType": "OnDemand",
                "status": "Completed",
                "startTime": "2024-01-16T10:00:00Z",
                "endTime": None,  # will calculate None seconds
                "serviceExceptionJson": None,
                "requestId": "191cc73a-fc75-4eff-9ffe-67aa5290d7f4",
                "refreshAttempts": None,
            },
        ]
        schema = [
            ("id", "Id", "long"),
            ("refreshType", "RefreshType", "string"),
            ("status", "Status", "string"),
            (
                lambda d: (d["endTime"] - d["startTime"]) / pd.Timedelta(seconds=1),
                "Seconds",
                "long",
            ),
            ("startTime", "StartTime", "timestamp"),
            ("endTime", "EndTime", "timestamp"),
            ("serviceExceptionJson", "Error", "string"),
            ("requestId", "RequestId", "string"),
            ("refreshAttempts", "RefreshAttempts", "string"),
        ]
        expected = pd.DataFrame(
            {
                "Id": [1, 2, 3],
                "RefreshType": ["OnDemand", "ViaApi", "ViaEnhancedApi"],
                "Status": ["Completed", "Completed", "Completed"],
                "Seconds": [
                    None,
                    300,
                    7200,
                ],  # despite the daylight saving change it
                # correctly shows 7200 seconds !
                "StartTimeLocal": [
                    datetime(2024, 1, 16, 11, 0),
                    datetime(2024, 2, 26, 11, 0),
                    datetime(2024, 3, 31, 1, 0),
                ],
                "EndTimeLocal": [
                    None,
                    datetime(2024, 2, 26, 11, 5),
                    datetime(2024, 3, 31, 4, 0),
                ],
                "Error": [None, None, None],
                "RequestId": [
                    "191cc73a-fc75-4eff-9ffe-67aa5290d7f4",
                    "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                    "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                ],
                "RefreshAttempts": None,
            }
        )
        expected["Id"] = expected["Id"].astype("Int64")
        expected["Seconds"] = expected["Seconds"].astype("Int64")

        # Act
        sut = SparkPandasDataFrame(
            json,
            schema,
            indexing_columns="Id",
            sorting_columns=["StartTimeLocal", "Id"],
            local_timezone_name="Europe/Copenhagen",
        )

        pd.options.display.max_columns = None
        pd.options.display.max_rows = None

        # Assert
        assert_frame_equal(
            expected.reset_index(drop=True), sut.get_pandas_df().reset_index(drop=True)
        )

    def test_parse_time_success(self):
        # Arrange
        data = [
            (None, None),
            (float("NaN"), None),
            ("2024-05-02", datetime(2024, 5, 2, 0, 0, 0, 0, tzinfo=None)),
            ("2024-05-03T13:35", datetime(2024, 5, 3, 13, 35, 0, 0, tzinfo=None)),
            (
                "2024-05-04T15:35:28+00:00",
                datetime(2024, 5, 4, 15, 35, 28, 0, tzinfo=None),
            ),
            (
                "2024-05-05T17:15:28.232223",
                datetime(2024, 5, 5, 17, 15, 28, 0, tzinfo=None),
            ),
            (
                "2024-05-05T21:33:04.232223+02:00",
                datetime(2024, 5, 5, 19, 33, 4, 0, tzinfo=None),
            ),
            (
                "2024-05-07T08:46:17.230735322332Z",
                datetime(2024, 5, 7, 8, 46, 17, 0, tzinfo=None),
            ),
        ]

        # Act
        for value, result in data:
            with self.subTest(value=value, result=result):
                # Act
                expected_result = SparkPandasDataFrame._parse_time(value)

                # Assert
                self.assertEqual(expected_result, result)
