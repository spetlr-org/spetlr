import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pandas as pd
from pandas.testing import assert_frame_equal
from pytz import utc

from spetlr.exceptions import SpetlrException
from spetlr.power_bi.PowerBi import PowerBi
from spetlr.power_bi.PowerBiClient import PowerBiClient
from spetlr.power_bi.PowerBiException import PowerBiException


class TestPowerBi(unittest.TestCase):
    @patch("requests.get")
    def test_verify_workspace_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [{"id": "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0", "name": "Finance"}]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_name="Finance")
        sut.powerbi_url = "test/"

        # Act
        result = sut._verify_workspace()

        # Assert
        self.assertTrue(result)
        self.assertEqual("614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0", sut.workspace_id)

    @patch("requests.get")
    def test_verify_workspace_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "error"
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_name="Finance")
        sut.powerbi_url = "test/"

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._verify_workspace()

        # Assert
        self.assertIn("Failed to fetch workspaces", str(context.exception))

    @patch("requests.get")
    def test_verify_dataset_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {"id": "b1f0a07e-e348-402c-a2b2-11f3e31181ce", "name": "Invoicing"}
            ]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_name="Finance",
            dataset_name="Invoicing",
        )
        sut.powerbi_url = "test/"

        # Act
        result = sut._verify_dataset()

        # Assert
        self.assertTrue(result)
        self.assertEqual("b1f0a07e-e348-402c-a2b2-11f3e31181ce", sut.dataset_id)

    @patch("requests.get")
    def test_verify_dataset_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "error"
        mock_get.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_name="Finance",
            dataset_name="Invoicing",
        )
        sut.powerbi_url = "test/"

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._verify_dataset()

        # Assert
        self.assertIn("Failed to fetch datasets", str(context.exception))

    @patch("requests.get")
    def test_get_refresh_history_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "id": 1,
                    "refreshType": "OnDemand",
                    "status": "Completed",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",  # winter time: 1 hour
                    "serviceExceptionJson": None,  # difference from UTC
                    "requestId": "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                    "refreshAttempts": None,
                },
                {
                    "id": 2,
                    "refreshType": "ViaEnhancedApi",
                    "status": "Completed",
                    "startTime": "2024-03-31T00:00:00Z",  # summer time: change from 1
                    "endTime": "2024-03-31T02:00:00Z",  # hour to 2 hours difference
                    "serviceExceptionJson": None,
                    "requestId": "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                    "refreshAttempts": None,
                },
            ]
        }

        def requests_get(url, headers):
            nonlocal mock_response
            if url.endswith("refreshes") and headers == "api_header":
                return mock_response
            raise ValueError("Unknown URL!")

        mock_get.side_effect = requests_get

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            local_timezone_name="Europe/Copenhagen",
        )
        sut.api_header = "api_header"
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True
        expected = pd.DataFrame(
            {
                "RefreshType": ["OnDemand", "ViaEnhancedApi"],
                "Status": ["Completed", "Completed"],
                "Seconds": [
                    300,
                    7200,
                ],  # despite the daylight saving change it
                # correctly shows 7200 seconds !
                "StartTimeLocal": [
                    datetime(2024, 2, 26, 11, 0),
                    datetime(2024, 3, 31, 1, 0),
                ],
                "EndTimeLocal": [
                    datetime(2024, 2, 26, 11, 5),
                    datetime(2024, 3, 31, 4, 0),
                ],
                "Error": [None, None],
                "RequestId": [
                    "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                    "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                ],
                "RefreshId": [1, 2],
                "RefreshAttempts": None,
            }
        )
        expected["RefreshId"] = expected["RefreshId"].astype("Int64")
        expected["Seconds"] = expected["Seconds"].astype("Int64")

        # Act
        result = sut._combine_dataframes(sut._get_refresh_history)

        # Assert
        self.assertIsNotNone(result)
        assert_frame_equal(expected, result.get_pandas_df())

    @patch("requests.get")
    def test_get_refresh_history_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404  # dataset or workspace not found
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._combine_dataframes(sut._get_refresh_history)

        # Assert
        self.assertIn(
            "The specified dataset or workspace cannot be found", str(context.exception)
        )

    @patch("requests.post")
    def test_get_partition_tables_success(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[ID]": 1,
                                    "[TableID]": 1001,
                                    "[Name]": "Invoices",
                                    "[RefreshedTime]": "2024-02-26T10:00:00Z",
                                    "[ModifiedTime]": "2024-02-26T10:05:00Z",
                                    "[Description]": "contains invoices",
                                    "[ErrorMessage]": None,
                                },
                                {
                                    "[ID]": 2,
                                    "[TableID]": 1002,
                                    "[Name]": "Customers",
                                    "[RefreshedTime]": "2024-02-26T15:00:00Z",
                                    "[ModifiedTime]": "2024-02-26T15:05:00Z",
                                    "[Description]": "contains customers",
                                    "[ErrorMessage]": "Refresh error",
                                },
                            ]
                        }
                    ]
                }
            ]
        }
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            local_timezone_name="Europe/Copenhagen",
        )
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True
        expected = pd.DataFrame(
            {
                "TableName": ["Invoices", "Customers"],
                "RefreshTimeLocal": [
                    datetime(2024, 2, 26, 11, 0),
                    datetime(2024, 2, 26, 16, 0),
                ],
                "ModifiedTimeLocal": [
                    datetime(2024, 2, 26, 11, 5),
                    datetime(2024, 2, 26, 16, 5),
                ],
                "Description": ["contains invoices", "contains customers"],
                "ErrorMessage": [None, "Refresh error"],
                "Id": [1, 2],
                "TableId": [1001, 1002],
            }
        ).sort_values("TableName")
        expected["Id"] = expected["Id"].astype("Int64")
        expected["TableId"] = expected["TableId"].astype("Int64")

        # Act
        result = sut._combine_dataframes(sut._get_partition_tables)

        # Assert
        self.assertIsNotNone(result)
        assert_frame_equal(
            expected, result.get_pandas_df()[list(expected.columns.values)]
        )

    @patch("requests.post")
    def test_get_partition_tables_failure(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True

        # Act
        # Also test if PowerBiException inherits from SpetlrException!
        with self.assertRaises(SpetlrException) as context:
            sut._combine_dataframes(sut._get_partition_tables)

        # Assert
        self.assertIn("Failed to fetch partition info", str(context.exception))

    @patch("requests.get")
    def test_combine_datasets_on_workspace_level_with_success(self, mock_get):
        # Arrange
        get_workspaces = {
            "value": [
                {
                    "id": "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "name": "Finance",
                    "isReadOnly": False,
                },
                {
                    "id": "5da990e9-089e-472c-a7fa-4fc3dd096d01",
                    "name": "CRM",
                    "isReadOnly": True,
                },
            ]
        }
        get_finance_datasets = {
            "value": [
                {
                    "id": "b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                    "name": "Invoicing",
                    "configuredBy": "james@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "2e848e9a-47a3-4b0e-a22a-af35507ec8c4",
                    "name": "Reimbursement",
                    "configuredBy": "olivia@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "4de28a6f-f7d4-4186-a529-bf6c65e67b31",
                    "name": "Fees",
                    "configuredBy": "evelyn@contoso.com",
                    "isRefreshable": False,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "c3c7591a-35bf-4f25-b03c-9c5ed6cdae14",
                    "name": "Fraud",
                    "configuredBy": "evelyn@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": True,
                    "isEffectiveIdentityRolesRequired": False,
                },
            ]
        }
        get_crm_datasets = {
            "value": [
                {
                    "id": "4869cc38-646c-45b6-89fd-62615beb9853",
                    "name": "Strategy",
                    "configuredBy": "mark@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": True,
                },
                {
                    "id": "68a2565d-4959-4421-b91c-bafe792796e1",
                    "name": "Strategy",
                    "configuredBy": "amelia@contoso.com",
                    "isRefreshable": False,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": True,
                },
            ]
        }

        def requests_get(url, headers):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = None
            if headers == "api_header":
                if url.endswith("groups"):
                    mock_response.json.return_value = get_workspaces
                elif (
                    url.endswith("datasets")
                    and "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0" in url
                ):
                    mock_response.json.return_value = get_finance_datasets
                elif (
                    url.endswith("datasets")
                    and "5da990e9-089e-472c-a7fa-4fc3dd096d01" in url
                ):
                    mock_response.json.return_value = get_crm_datasets
                if mock_response.json.return_value:
                    return mock_response
            raise ValueError("Unknown URL! " + url)

        mock_get.side_effect = requests_get

        sut = PowerBi(
            PowerBiClient(),
            local_timezone_name="Europe/Copenhagen",
        )
        sut.api_header = "api_header"
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True
        expected = pd.DataFrame(
            {
                "WorkspaceName": [
                    "CRM",
                    "CRM",
                    "Finance",
                    "Finance",
                    "Finance",
                    "Finance",
                ],
                "DatasetName": [
                    "Strategy",
                    "Strategy",
                    "Fees",
                    "Fraud",
                    "Invoicing",
                    "Reimbursement",
                ],
                "ConfiguredBy": [
                    "amelia@contoso.com",
                    "mark@contoso.com",
                    "evelyn@contoso.com",
                    "evelyn@contoso.com",
                    "james@contoso.com",
                    "olivia@contoso.com",
                ],
                "IsRefreshable": [False, True, False, True, True, True],
                "AddRowsApiEnabled": [False, False, False, False, False, False],
                "IsEffectiveIdentityRequired": [
                    False,
                    False,
                    False,
                    True,
                    False,
                    False,
                ],
                "IsEffectiveIdentityRolesRequired": [
                    True,
                    True,
                    False,
                    False,
                    False,
                    False,
                ],
                "IsOnPremGatewayRequired": [False, False, False, False, False, False],
                "DatasetId": [
                    "68a2565d-4959-4421-b91c-bafe792796e1",
                    "4869cc38-646c-45b6-89fd-62615beb9853",
                    "4de28a6f-f7d4-4186-a529-bf6c65e67b31",
                    "c3c7591a-35bf-4f25-b03c-9c5ed6cdae14",
                    "b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                    "2e848e9a-47a3-4b0e-a22a-af35507ec8c4",
                ],
                "WorkspaceId": [
                    "5da990e9-089e-472c-a7fa-4fc3dd096d01",
                    "5da990e9-089e-472c-a7fa-4fc3dd096d01",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                ],
            }
        )

        # Act
        result = sut._combine_datasets()

        # Assert
        self.assertIsNotNone(result)
        assert_frame_equal(expected, result.get_pandas_df())

    @patch("requests.get")
    def test_combine_dataframes_on_workspace_level_with_success(self, mock_get):
        # Arrange
        get_workspaces = {
            "value": [
                {
                    "id": "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "name": "Finance",
                    "isReadOnly": False,
                },
                {
                    "id": "5da990e9-089e-472c-a7fa-4fc3dd096d01",
                    "name": "CRM",
                    "isReadOnly": True,  # excluded
                },
            ]
        }
        get_finance_datasets = {
            "value": [
                {
                    "id": "b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                    "name": "Invoicing",
                    "configuredBy": "james@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "2e848e9a-47a3-4b0e-a22a-af35507ec8c4",
                    "name": "Reimbursement",
                    "configuredBy": "olivia@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "4869cc38-646c-45b6-89fd-62615beb9853",  # excluded HTTP 401
                    "name": "Strategy",
                    "configuredBy": "mark@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "68a2565d-4959-4421-b91c-bafe792796e1",
                    "name": "Strategy",
                    "configuredBy": "amelia@contoso.com",  # exclude_creators
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "8e4413eb-ba48-4a29-a0c2-fc80272d858e",
                    "name": "Test",
                    "configuredBy": None,  # exclude_creators
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "4de28a6f-f7d4-4186-a529-bf6c65e67b31",
                    "name": "Fees",
                    "configuredBy": "evelyn@contoso.com",
                    "isRefreshable": False,  # excluded
                    "isEffectiveIdentityRequired": False,
                    "isEffectiveIdentityRolesRequired": False,
                },
                {
                    "id": "c3c7591a-35bf-4f25-b03c-9c5ed6cdae14",
                    "name": "Fraud",
                    "configuredBy": "evelyn@contoso.com",
                    "isRefreshable": True,
                    "isEffectiveIdentityRequired": True,  # excluded, only in unit test!
                    "isEffectiveIdentityRolesRequired": False,
                },
            ]
        }
        get_invoicing_refresh_history = {
            "value": [
                {
                    "id": 2,
                    "refreshType": "ViaEnhancedApi",
                    "status": "Completed",
                    "startTime": "2024-03-31T00:00:00Z",  # summer time: change from 1
                    "endTime": "2024-03-31T02:00:00Z",  # hour to 2 hours difference
                    "serviceExceptionJson": None,
                    "requestId": "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                    "refreshAttempts": None,
                },
                {
                    "id": 1,
                    "refreshType": "OnDemand",
                    "status": "Completed",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",  # winter time: 1 hour
                    "serviceExceptionJson": None,  # difference from UTC
                    "requestId": "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                    "refreshAttempts": None,
                },
            ]
        }
        get_reimbursement_refresh_history = {
            "value": [
                {
                    "id": 2,
                    "refreshType": "ViaEnhancedApi",
                    "status": "Completed",
                    "startTime": "2024-02-24T10:00:00Z",
                    "endTime": "2024-02-24T10:09:00Z",
                    "serviceExceptionJson": None,
                    "requestId": "8d315d90-6105-450b-af2b-8654b962db98",
                    "refreshAttempts": None,
                },
                {
                    "id": 1,
                    "refreshType": "ViaApi",
                    "status": "Completed",
                    "startTime": "2024-02-23T10:00:00Z",
                    "endTime": "2024-02-23T10:10:00Z",
                    "serviceExceptionJson": None,
                    "requestId": "11abf327-6e6b-4cdf-a9a0-f443d83680ec",
                    "refreshAttempts": None,
                },
            ]
        }

        def requests_get(url, headers):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = None
            if headers == "api_header":
                if url.endswith("groups"):
                    mock_response.json.return_value = get_workspaces
                elif (
                    url.endswith("datasets")
                    and "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0" in url
                ):
                    mock_response.json.return_value = get_finance_datasets
                elif (
                    url.endswith("refreshes")
                    and "b1f0a07e-e348-402c-a2b2-11f3e31181ce" in url
                ):
                    mock_response.json.return_value = get_invoicing_refresh_history
                elif (
                    url.endswith("refreshes")
                    and "2e848e9a-47a3-4b0e-a22a-af35507ec8c4" in url
                ):
                    mock_response.json.return_value = get_reimbursement_refresh_history
                elif (
                    url.endswith("refreshes")
                    and "4869cc38-646c-45b6-89fd-62615beb9853" in url
                ):
                    mock_response.status_code = 401
                if mock_response.json.return_value or mock_response.status_code != 200:
                    return mock_response
            raise ValueError("Unknown URL! " + url)

        mock_get.side_effect = requests_get

        sut = PowerBi(
            PowerBiClient(),
            local_timezone_name="Europe/Copenhagen",
            exclude_creators=["Amelia@contoso.com", None],
        )
        sut.api_header = "api_header"
        sut.powerbi_url = "test/"
        sut._get_access_token = lambda: True
        expected = pd.DataFrame(
            {
                "WorkspaceName": ["Finance", "Finance", "Finance", "Finance"],
                "DatasetName": [
                    "Invoicing",
                    "Invoicing",
                    "Reimbursement",
                    "Reimbursement",
                ],
                "RefreshType": [
                    "ViaEnhancedApi",
                    "OnDemand",
                    "ViaEnhancedApi",
                    "ViaApi",
                ],
                "Status": ["Completed", "Completed", "Completed", "Completed"],
                "Seconds": [7200, 300, 540, 600],
                "StartTimeLocal": [
                    datetime(2024, 3, 31, 1, 0),
                    datetime(2024, 2, 26, 11, 0),
                    datetime(2024, 2, 24, 11, 0),
                    datetime(2024, 2, 23, 11, 0),
                ],
                "EndTimeLocal": [
                    datetime(2024, 3, 31, 4, 0),
                    datetime(2024, 2, 26, 11, 5),
                    datetime(2024, 2, 24, 11, 9),
                    datetime(2024, 2, 23, 11, 10),
                ],
                "Error": [None, None, None, None],
                "RequestId": [
                    "aec28227-f7af-4c2d-a4e6-fcb01cd570ec",
                    "74d25c0b-0473-4dd9-96ff-3ca737b072a7",
                    "8d315d90-6105-450b-af2b-8654b962db98",
                    "11abf327-6e6b-4cdf-a9a0-f443d83680ec",
                ],
                "RefreshId": [2, 1, 2, 1],
                "RefreshAttempts": None,
                "WorkspaceId": [
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                    "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                ],
                "DatasetId": [
                    "b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                    "b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                    "2e848e9a-47a3-4b0e-a22a-af35507ec8c4",
                    "2e848e9a-47a3-4b0e-a22a-af35507ec8c4",
                ],
            }
        )
        expected["RefreshId"] = expected["RefreshId"].astype("Int64")
        expected["Seconds"] = expected["Seconds"].astype("Int64")

        # Act
        result = sut._combine_dataframes(
            sut._get_refresh_history,
            skip_read_only=True,
            skip_not_refreshable=True,
            skip_effective_identity=True,
        )

        # Assert
        self.assertIsNotNone(result)
        assert_frame_equal(expected, result.get_pandas_df())

    @patch("requests.get")
    def test_get_last_refresh_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T17:00:00Z",
                    "endTime": None,
                    "status": "Unknown",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "ViaApi",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh(deep_check=True)

        # Assert
        self.assertTrue(result)
        self.assertEqual("Completed", sut.last_status)
        self.assertFalse(sut.is_enhanced)
        self.assertIsNone(sut.last_exception)
        self.assertEqual("2024-02-26 10:05:00+00:00", str(sut.last_refresh_utc))
        # average of ViaApi only
        self.assertEqual(5 * 60, sut.last_duration_in_seconds)

    @patch("requests.get")
    def test_get_last_refresh_deep_check_with_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "requestId": "5",
                    "id": "4",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T17:00:00Z",
                    "endTime": "2024-02-27T17:05:00Z",
                    "status": "Unknown",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "4",
                    "id": "4",
                    "refreshType": "ViaApi",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "3",
                    "id": "3",
                    "refreshType": "OnDemand",
                    "startTime": "2024-02-25T10:00:00Z",
                    "endTime": "2024-02-25T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaEnhancedApi",
                    "startTime": "2024-02-24T10:00:00Z",
                    "endTime": "2024-02-24T10:09:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "ViaApi",
                    "startTime": "2024-02-23T10:00:00Z",
                    "endTime": "2024-02-23T10:10:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._connect = lambda: True
        expected_duration = int(7.5 * 60)  # average duration from all ViaApi

        # Act
        result = sut._get_last_refresh(deep_check=True)

        # Assert
        self.assertTrue(result)
        self.assertEqual("Completed", sut.last_status)
        self.assertFalse(sut.is_enhanced)
        self.assertIsNone(sut.last_exception)
        self.assertEqual("2024-02-26 10:05:00+00:00", str(sut.last_refresh_utc))
        # average of ViaApi only
        self.assertEqual(expected_duration, sut.last_duration_in_seconds)

    @patch("requests.get")
    def test_get_last_refresh_deep_check_normal_with_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "requestId": "5",
                    "id": "4",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T17:00:00Z",
                    "endTime": "2024-02-27T17:05:00Z",
                    "status": "Unknown",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "4",
                    "id": "4",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T16:00:00Z",
                    "endTime": "2024-02-27T16:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "3",
                    "id": "3",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T15:00:00Z",
                    "endTime": "2024-02-27T15:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaEnhancedApi",  # skip an enhanced refresh
                    "startTime": "2024-02-27T14:00:00Z",
                    "endTime": "2024-02-27T14:09:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "ViaEnhancedApi",  # the last is ok
                    "startTime": "2024-02-27T10:00:00Z",
                    "endTime": "2024-02-27T10:11:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh(deep_check=True)
        expected_duration = 0  # average duration from all ViaApi

        # Assert
        self.assertTrue(result)
        self.assertEqual("Completed", sut.last_status)
        self.assertTrue(sut.is_enhanced)
        self.assertIsNone(sut.last_exception)
        self.assertEqual("2024-02-27 10:11:00+00:00", str(sut.last_refresh_utc))
        # average of ViaApi only
        self.assertEqual(expected_duration, sut.last_duration_in_seconds)

    @patch("requests.get")
    @patch("requests.post")
    def test_get_last_refresh_with_tables_success(self, mock_post, mock_get):
        # Arrange
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "value": [
                {
                    "requestId": "3",
                    "id": "3",
                    "refreshType": "ViaEnhancedApi",
                    "startTime": "2024-02-27T17:00:00Z",
                    "endTime": "2024-02-27T17:05:00Z",
                    "status": "Unknown",  # skip as an enhanced refresh is in progress
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaEnhancedApi",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "OnDemand",
                    "startTime": "2024-02-25T10:00:00Z",
                    "endTime": "2024-02-25T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_post_response = Mock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[ID]": 1,
                                    "[TableID]": 1001,
                                    "[Name]": "Invoices",
                                    "[RefreshedTime]": "2024-02-26T10:50:00+01:00",
                                    "[ModifiedTime]": "2024-02-26T10:55:00+01:00",
                                    "[Description]": "contains invoices",
                                    "[ErrorMessage]": None,
                                },
                                {
                                    "[ID]": 2,
                                    "[TableID]": 1002,
                                    "[Name]": "Customers",
                                    "[RefreshedTime]": "2024-02-26T15:50:00+01:00",
                                    "[ModifiedTime]": "2024-02-26T15:55:00+01:00",
                                    "[Description]": "contains customers",
                                    "[ErrorMessage]": None,
                                },
                            ]
                        }
                    ]
                }
            ]
        }
        mock_get.return_value = mock_get_response
        mock_post.return_value = mock_post_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            table_names=["Invoices", "Customers"],
        )
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh(deep_check=True)

        # Assert
        self.assertTrue(result)
        self.assertEqual("Completed", sut.last_status)
        self.assertTrue(sut.is_enhanced)
        self.assertEqual("Invoices", sut.table_name)
        self.assertIsNone(sut.last_exception)
        self.assertEqual("2024-02-26 09:50:00+00:00", str(sut.last_refresh_utc))
        self.assertEqual(0, sut.last_duration_in_seconds)  # tables were specified!

    @patch("requests.get")
    @patch("requests.post")
    def test_get_last_refresh_with_failed_tables_success(self, mock_post, mock_get):
        # Arrange
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "value": [
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaApi",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "OnDemand",
                    "startTime": "2024-02-25T10:00:00Z",
                    "endTime": "2024-02-25T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_post_response = Mock()
        mock_post_response.status_code = 200
        mock_post_response.json.return_value = {
            "results": [
                {
                    "tables": [
                        {
                            "rows": [
                                {
                                    "[ID]": 1,
                                    "[TableID]": 1001,
                                    "[Name]": "Invoices",
                                    "[RefreshedTime]": "2024-02-26T10:50:00+01:00",
                                    "[ModifiedTime]": "2024-02-26T10:55:00+01:00",
                                    "[Description]": "contains invoices",
                                    "[ErrorMessage]": None,
                                },
                                {
                                    "[ID]": 2,
                                    "[TableID]": 1002,
                                    "[Name]": "Customers",
                                    "[RefreshedTime]": "2024-02-26T15:50:00+01:00",
                                    "[ModifiedTime]": "2024-02-26T15:55:00+01:00",
                                    "[Description]": "contains customers",
                                    "[ErrorMessage]": "Refresh error",
                                },
                            ]
                        }
                    ]
                }
            ]
        }
        mock_get.return_value = mock_get_response
        mock_post.return_value = mock_post_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            table_names=["Invoices", "Customers"],
        )
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh()

        # Assert
        self.assertTrue(result)
        self.assertEqual("Failed", sut.last_status)
        self.assertFalse(sut.is_enhanced)
        self.assertEqual("Customers", sut.table_name)
        self.assertEqual("Refresh error", sut.last_exception)
        self.assertIsNone(sut.last_refresh_utc)

    @patch("requests.get")
    @patch("requests.post")
    def test_get_last_refresh_with_unauthorized_tables_success(
        self, mock_post, mock_get
    ):
        # Arrange
        mock_get_response = Mock()
        mock_get_response.status_code = 200
        mock_get_response.json.return_value = {
            "value": [
                {
                    "requestId": "2",
                    "id": "2",
                    "refreshType": "ViaApi",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "OnDemand",
                    "startTime": "2024-02-25T10:00:00Z",
                    "endTime": "2024-02-25T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                },
            ]
        }
        mock_post_response = Mock()
        mock_post_response.status_code = 401
        mock_get.return_value = mock_get_response
        mock_post.return_value = mock_post_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            table_names=["Invoices", "Customers"],
        )
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh()

        # Assert
        self.assertTrue(result)
        self.assertEqual("Completed", sut.last_status)
        self.assertFalse(sut.is_enhanced)
        self.assertIsNone(sut.table_name)
        self.assertIsNone(sut.last_exception)
        self.assertEqual("2024-02-26 10:05:00+00:00", str(sut.last_refresh_utc))
        self.assertEqual(0, sut.last_duration_in_seconds)  # tables were specified!

    @patch("requests.get")
    def test_get_last_refresh_empty(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": []}
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut.last_status = "test"
        sut.last_duration_in_seconds = 5
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh()

        # Assert
        self.assertTrue(result)
        self.assertIsNone(sut.last_exception)
        self.assertFalse(sut.is_enhanced)
        self.assertIsNone(sut.last_status)  # must be cleared!
        self.assertEqual(5, sut.last_duration_in_seconds)  # must be kept unchanged!

    @patch("requests.get")
    def test_get_last_refresh_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404  # dataset or workspace not found
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test/"
        sut._connect = lambda: True

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._get_last_refresh()

        # Assert
        self.assertIn(
            "The specified dataset or workspace cannot be found", str(context.exception)
        )

    def test_verify_last_refresh_success(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            max_minutes_after_last_refresh=5,
            local_timezone_name="Europe/Copenhagen",
        )
        sut.last_status = "Completed"
        sut.table_name = "TestTable"
        sut.last_refresh_utc = datetime.now(utc) - timedelta(minutes=1)
        sut.last_refresh_str = str(sut.last_refresh_utc)

        # Act
        result = sut._verify_last_refresh()

        # Assert
        self.assertTrue(result)

    def test_verify_last_refresh_failure(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="test",
            dataset_id="test",
            max_minutes_after_last_refresh=5,
            local_timezone_name="Europe/Copenhagen",
        )
        sut.last_status = "Completed"
        sut.table_name = "TestTable"
        sut.last_refresh_str = "2024-05-01"
        sut.last_refresh_utc = datetime.now(utc) - timedelta(minutes=10)

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._verify_last_refresh()

        # Assert
        self.assertIn("finished more than", str(context.exception))
        self.assertIn("TestTable", str(context.exception))

    def test_get_refresh_argument_json_default(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
        )

        # Act
        result = sut._get_refresh_argument_json(True)

        # Assert
        self.assertIsNone(result)

    def test_get_refresh_argument_json_combination_failure(self):
        # Act
        with self.assertRaises(ValueError) as context:
            PowerBi(
                PowerBiClient(),
                workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
                dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
                table_names=["Invoices", "Customers"],
                mail_on_failure=True,
            )

        # Assert
        self.assertIn("cannot be combined", str(context.exception))

    def test_get_refresh_argument_json_without_tables_and_without_wait(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            number_of_retries=2,  # should be ignored without tables
            mail_on_failure=True,
        )
        expected_result = "MailOnFailure"

        # Act
        result = sut._get_refresh_argument_json(with_wait=False)

        # Assert
        self.assertIsNotNone(result)
        self.assertFalse("objects" in result)
        self.assertFalse("retryCount" in result)
        self.assertTrue("notifyOption" in result)
        self.assertEqual(expected_result, result["notifyOption"])

    def test_get_refresh_argument_json_with_tables_and_without_wait(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            table_names=["Invoices", "Customers"],
            number_of_retries=2,  # should not be ignored
        )
        expected_result1 = 2
        expected_result2 = [{"table": "Invoices"}, {"table": "Customers"}]

        # Act
        result = sut._get_refresh_argument_json(with_wait=False)

        # Assert
        self.assertIsNotNone(result)
        self.assertTrue("objects" in result)
        self.assertTrue("retryCount" in result)
        self.assertEqual(expected_result1, result["retryCount"])
        self.assertEqual(expected_result2, result["objects"])

    def test_get_refresh_argument_json_with_tables_and_with_wait(self):
        # Arrange
        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            table_names=["Invoices", "Customers"],
            number_of_retries=2,  # should be ignored if with_wait=True
        )
        expected_result = [{"table": "Invoices"}, {"table": "Customers"}]

        # Act
        result = sut._get_refresh_argument_json(with_wait=True)

        # Assert
        self.assertIsNotNone(result)
        self.assertTrue("objects" in result)
        self.assertFalse("retryCount" in result)
        self.assertEqual(expected_result, result["objects"])

    @patch("requests.post")
    def test_trigger_new_refresh_success(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
        )
        sut.last_status = "Completed"
        sut.powerbi_url = "test/"

        # Act
        result = sut._trigger_new_refresh()

        # Assert
        self.assertTrue(result)

    @patch("requests.post")
    def test_trigger_new_refresh_failure(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "error"
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
        )
        sut.last_status = "Completed"
        sut.powerbi_url = "test/"

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut._trigger_new_refresh()

        # Assert
        self.assertIn("Failed to trigger a refresh", str(context.exception))

    def test_get_seconds_to_wait_first(self):
        # Arrange
        sut = PowerBi(PowerBiClient())
        sut.last_duration_in_seconds = 0
        elapsed = 5
        expected_result = 15

        # Act
        result = sut._get_seconds_to_wait(elapsed)

        # Assert
        self.assertEqual(expected_result, result)

    def test_get_seconds_to_wait_next(self):
        # Arrange
        sut = PowerBi(PowerBiClient())
        sut.last_duration_in_seconds = 15 * 60
        elapsed = 5
        expected_result = 5 * 60

        # Act
        result = sut._get_seconds_to_wait(elapsed)

        # Assert
        self.assertEqual(expected_result, result)

    def test_get_seconds_to_wait_exceeding_timeout(self):
        # Arrange
        sut = PowerBi(PowerBiClient(), timeout_in_seconds=90)
        sut.last_duration_in_seconds = 15 * 60
        elapsed = 5
        expected_result = 90 - elapsed

        # Act
        result = sut._get_seconds_to_wait(elapsed)

        # Assert
        self.assertEqual(expected_result, result)

    @patch("requests.post")
    def test_refresh_no_restart_no_tables_success(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 501  # not called, so the error is irrelevant!
        mock_response.text = "error"
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            timeout_in_seconds=1,
            number_of_retries=0,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            sut.is_enhanced = False
            counter += 1
            if counter == 1:
                sut.last_status = "Unknown"
            elif counter == 2:
                sut.last_status = "Completed"
                sut.last_refresh_utc = datetime.now(utc)
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        sut.refresh()  # No exception!

        # Assert
        self.assertFalse(mock_post.called)

    @patch("requests.post")
    def test_refresh_restart_no_tables_timeout(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202  # called only when restart triggered!
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            timeout_in_seconds=1,
            number_of_retries=0,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            sut.is_enhanced = True
            counter += 1
            if counter == 1:
                sut.last_status = "Unknown"
            elif counter == 2:
                sut.last_status = "Completed"  # one restart, then timeout!
                sut.last_refresh_utc = datetime(2024, 5, 14, 9, 17, tzinfo=utc)
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut.refresh()

        # Assert
        self.assertEqual(1, mock_post.call_count)
        self.assertIn("still in progress", str(context.exception))

    @patch("requests.post")
    def test_refresh_no_restart_with_tables_success(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 401  # not called, so the error is irrelevant!
        mock_response.text = "error"
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            table_names=["Invoices", "Customers"],
            timeout_in_seconds=1,
            number_of_retries=0,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            counter += 1
            if counter == 1:
                sut.last_status = "Unknown"
                sut.is_enhanced = False
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        sut.refresh()  # No exception!

        # Assert
        self.assertFalse(mock_post.called)

    @patch("requests.post")
    def test_refresh_restart_with_tables_timeout(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202  # called only whe restart triggered!
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            table_names=["Invoices", "Customers"],
            timeout_in_seconds=1,
            number_of_retries=0,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            sut.is_enhanced = True
            counter += 1
            if counter == 1:
                sut.last_status = "Unknown"
            elif counter == 2:
                sut.last_status = "Completed"  # one restart, then timeout!
                sut.last_refresh_utc = datetime(2024, 5, 14, 9, 17, tzinfo=utc)
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut.refresh()

        # Assert
        self.assertEqual(1, mock_post.call_count)
        self.assertIn("still in progress", str(context.exception))

    @patch("requests.post")
    def test_refresh_with_retry_timeout(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202  # called twice, in the beginning and on retry!
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            timeout_in_seconds=1,
            number_of_retries=1,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            sut.is_enhanced = True
            counter += 1
            if counter == 1:
                sut.last_status = "Completed"  # refresh triggered
                sut.last_refresh_utc = datetime(2024, 5, 14, 9, 17, tzinfo=utc)
            elif counter == 2:
                sut.last_status = "Failed"  # refresh triggered again, but then timeout!
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut.refresh()

        # Assert
        self.assertEqual(2, mock_post.call_count)
        self.assertIn("still in progress", str(context.exception))

    @patch("requests.post")
    def test_refresh_not_retry_failure(self, mock_post):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202  # called only in the beginning!
        mock_post.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
            timeout_in_seconds=1,
            number_of_retries=0,
        )
        sut.powerbi_url = "test/"
        counter = 0

        def get_last_refresh():
            nonlocal sut, counter
            sut.table_name = None
            sut.last_refresh_str = None
            sut.is_enhanced = True
            counter += 1
            if counter == 1:
                sut.last_status = "Completed"
                sut.last_refresh_utc = datetime(2024, 5, 14, 9, 17, tzinfo=utc)
            elif counter == 2:
                sut.last_status = "Failed"  # the reason of the exception!
            else:
                raise ValueError("Called too many times")
            return True

        sut._get_last_refresh = get_last_refresh

        # Act
        with self.assertRaises(PowerBiException) as context:
            sut.refresh()

        # Assert
        self.assertEqual(1, mock_post.call_count)
        self.assertIn("Last refresh failed", str(context.exception))
