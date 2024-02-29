import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from pytz import utc

from spetlr.exceptions import SpetlrException
from spetlr.power_bi.PowerBi import PowerBi
from spetlr.power_bi.PowerBiClient import PowerBiClient


class TestPowerBi(unittest.TestCase):
    @patch("requests.get")
    def test_get_workspace_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [{"id": "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0", "name": "Finance"}]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_name="Finance")
        sut.powerbi_url = "test"

        # Act
        result = sut._get_workspace()

        # Assert
        self.assertTrue(result)
        self.assertEqual(sut.workspace_id, "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0")

    @patch("requests.get")
    def test_get_workspace_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "error"
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_name="Finance")
        sut.powerbi_url = "test"

        # Act
        with self.assertRaises(SpetlrException) as context:
            sut._get_workspace()

        # Assert
        self.assertIn("Failed to fetch workspaces", str(context.exception))

    @patch("requests.get")
    def test_get_dataset_success(self, mock_get):
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
        sut.powerbi_url = "test"

        # Act
        result = sut._get_dataset()

        # Assert
        self.assertTrue(result)
        self.assertEqual(sut.dataset_id, "b1f0a07e-e348-402c-a2b2-11f3e31181ce")

    @patch("requests.get")
    def test_get_dataset_failure(self, mock_get):
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
        sut.powerbi_url = "test"

        # Act
        with self.assertRaises(SpetlrException) as context:
            sut._get_dataset()

        # Assert
        self.assertIn("Failed to fetch datasets", str(context.exception))

    @patch("requests.get")
    def test_get_last_refresh_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "value": [
                {
                    "requestId": "1",
                    "id": "1",
                    "refreshType": "Manual",
                    "startTime": "2024-02-26T10:00:00Z",
                    "endTime": "2024-02-26T10:05:00Z",
                    "status": "Completed",
                    "serviceExceptionJson": None,
                }
            ]
        }
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test"
        sut._connect = lambda: True

        # Act
        result = sut._get_last_refresh()

        # Assert
        self.assertTrue(result)
        self.assertEqual(sut.last_status, "Completed")
        self.assertIsNone(sut.last_exception)
        self.assertEqual(str(sut.last_refresh_utc), "2024-02-26 10:05:00+00:00")
        self.assertEqual(sut.last_duration_in_seconds, 5 * 60)

    @patch("requests.get")
    def test_get_last_refresh_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404  # dataset or workspace not found
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient(), workspace_id="test", dataset_id="test")
        sut.powerbi_url = "test"
        sut._connect = lambda: True

        # Act
        with self.assertRaises(SpetlrException) as context:
            sut._get_last_refresh()

        # Assert
        self.assertIn(
            "The specified dataset or workspace cannot be found",
            str(context.exception),
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
        sut.last_refresh_utc = datetime.now(utc) - timedelta(minutes=1)

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
        sut.last_refresh_utc = datetime.now(utc) - timedelta(minutes=10)

        # Act
        with self.assertRaises(SpetlrException) as context:
            sut._verify_last_refresh()

        # Assert
        self.assertIn("Last refresh finished more than", str(context.exception))

    @patch("requests.post")
    def test_trigger_new_refresh_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202
        mock_get.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
        )
        sut.last_status = "Completed"
        sut.powerbi_url = "test"

        # Act
        result = sut._trigger_new_refresh()

        # Assert
        self.assertTrue(result)

    @patch("requests.post")
    def test_trigger_new_refresh_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "error"
        mock_get.return_value = mock_response

        sut = PowerBi(
            PowerBiClient(),
            workspace_id="614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0",
            dataset_id="b1f0a07e-e348-402c-a2b2-11f3e31181ce",
        )
        sut.last_status = "Completed"
        sut.powerbi_url = "test"

        # Act
        with self.assertRaises(SpetlrException) as context:
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
        self.assertEqual(result, expected_result)

    def test_get_seconds_to_wait_next(self):
        # Arrange
        sut = PowerBi(PowerBiClient())
        sut.last_duration_in_seconds = 15 * 60
        elapsed = 5
        expected_result = 5 * 60

        # Act
        result = sut._get_seconds_to_wait(elapsed)

        # Assert
        self.assertEqual(result, expected_result)

    def test_get_seconds_to_wait_exceeding_timeout(self):
        # Arrange
        sut = PowerBi(PowerBiClient(), timeout_in_seconds=90)
        sut.last_duration_in_seconds = 15 * 60
        elapsed = 5
        expected_result = 90 - elapsed

        # Act
        result = sut._get_seconds_to_wait(elapsed)

        # Assert
        self.assertEqual(result, expected_result)
