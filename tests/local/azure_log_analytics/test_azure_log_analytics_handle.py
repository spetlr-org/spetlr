import unittest
from datetime import date
from unittest import mock

from freezegun import freeze_time
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.time import dt_utc

from spetlr.azure_log_analytics import AzureLogAnalyticsHandle
from spetlr.utils import DataframeCreator


class TestAzureLogAnalyticsHandle(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.workspace_id = "4de5-8ic5"
        cls.resource = "/api/logs"

        schema = StructType(
            [
                StructField("col_1", IntegerType(), True),
                StructField("col_2", StringType(), True),
                StructField("col_3", TimestampType(), True),
                StructField("col_4", DateType(), True),
            ]
        )

        data = [
            (1, "first", dt_utc(2011, 1, 1, 8, 25, 25), date(2018, 3, 29)),
            (2, "second", dt_utc(2022, 2, 2, 18, 10, 10), date(2020, 2, 2)),
        ]

        cls.df = DataframeCreator.make(schema, data)

    def setUp(self) -> None:
        self.az_lah = AzureLogAnalyticsHandle(
            log_analytics_workspace_id=self.workspace_id,
            shared_key="",
        )

    def test_create_uri_01(self) -> None:
        uri = self.az_lah._create_uri(self.resource)

        expected_uri = (
            f"https://{self.workspace_id}.ods.opinsights"
            + f".azure.com{self.resource}?api-version=2016-04-01"
        )

        self.assertEqual(uri, expected_uri)

    def test_create_body_02(self) -> None:
        body = self.az_lah._create_body(self.df)

        expected_body = (
            """[\n"""
            + """    {\n"""
            + """        "col_1": 1,\n"""
            + """        "col_2": "first",\n"""
            + """        "col_3": "2011-01-01T08:25:25",\n"""
            + """        "col_4": "2018-03-29"\n"""
            + """    },\n"""
            + """    {\n"""
            + """        "col_1": 2,\n"""
            + """        "col_2": "second",\n"""
            + """        "col_3": "2022-02-02T18:10:10",\n"""
            + """        "col_4": "2020-02-02"\n"""
            + """    }\n"""
            + """]"""
        )

        self.assertEqual(body, expected_body)

    @freeze_time("2020-06-15 12:10:10")
    def test_create_headers_03(self) -> None:
        shared_key = "1/a=2b3-c4d+5e"

        az_lah_no_log_type = AzureLogAnalyticsHandle(
            log_analytics_workspace_id=self.workspace_id,
            shared_key=shared_key,
        )

        method = "POST"
        content_type = "application/json"
        content_length = 80
        resource = self.resource

        headers = az_lah_no_log_type._create_headers(
            method, content_type, content_length, resource
        )

        expected_headers = {
            "content-type": "application/json",
            "Authorization": "SharedKey 4de5-8ic5:uAALir2LY"
            + "1gju1qtYw8ut1bL4V9p3FR+0CxJhHjRkbY=",
            "Log-Type": "Databricks",
            "x-ms-date": "Mon, 15 Jun 2020 12:10:10 GMT",
        }

        self.assertEqual(headers, expected_headers)

    def test_http_data_collector_api_post_200_ok_04(self) -> None:
        with mock.patch("requests.post") as mock_requests_post:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_requests_post.return_value = mock_response

            self.assertEqual(self.az_lah.api_post(self.df), 200)

    def test_http_data_collector_api_post_400_warning_05(self) -> None:
        with mock.patch("requests.post") as mock_requests_post:
            mock_response = mock.Mock()
            mock_response.status_code = 400
            mock_requests_post.return_value = mock_response

            # when the response code from the api != 200, then it should warn
            with self.assertWarns(UserWarning):
                self.assertEqual(self.az_lah.api_post(self.df), 400)

    def test_append_06(self) -> None:
        # tests that the method 'append' is an alias to the 'api_post' method
        self.assertEqual(
            AzureLogAnalyticsHandle.__dict__["append"],
            AzureLogAnalyticsHandle.__dict__["api_post"],
        )

    def test_read_07(self) -> None:
        # tests that the method 'read' is an alias to the 'api_get' method
        self.assertEqual(
            AzureLogAnalyticsHandle.__dict__["read"],
            AzureLogAnalyticsHandle.__dict__["api_get"],
        )

        with self.assertRaises(NotImplementedError):
            self.az_lah.read()

    def test_prepare_dataframe_08(self) -> None:
        pass  # the function is implicitly tested in test_create_body_02


if __name__ == "__main__":
    unittest.main()
