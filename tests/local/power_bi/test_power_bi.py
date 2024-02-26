import unittest
from datetime import datetime
from unittest.mock import Mock, patch

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

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "abc"
        sut.workspace_id = None
        sut.workspace_name = "Finance"

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

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "abc"
        sut.workspace_id = None
        sut.workspace_name = "Finance"

        # Act
        with self.assertRaises(Exception) as context:
            sut._get_workspace()

        # Assert
        self.assertIn("Failed to fetch workspaces!", str(context.exception))

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

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "abc"
        sut.workspace_id = "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0"
        sut.dataset_id = None
        sut.dataset_name = "Invoicing"

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

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "abc"
        sut.workspace_id = "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0"
        sut.dataset_id = None
        sut.dataset_name = "Invoicing"

        # Act
        with self.assertRaises(Exception) as context:
            sut._get_dataset()

        # Assert
        self.assertIn("Failed to fetch datasets!", str(context.exception))

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

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "test"
        sut.workspace_id = "test"
        sut.dataset_id = "test"
        sut._connect = lambda: None

        # Act
        result = sut._get_last_refresh()

        # Assert
        self.assertTrue(result)
        self.assertEqual(sut.last_status, "Completed")
        self.assertIsNone(sut.last_exception)
        self.assertEqual(str(sut.last_refresh_utc), "2024-02-27 10:05:00+00:00")
        self.assertEqual(sut.last_duration, 300)

    @patch("requests.get")
    def test_get_last_refresh_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404  # dataset or workspace not found
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient())
        sut.powerbi_url = "test"
        sut.workspace_id = "test"
        sut.dataset_id = "test"
        sut._connect = lambda: None

        # Act
        with self.assertRaises(Exception) as context:
            sut._get_last_refresh()

        # Assert
        self.assertEqual(
            str(context.exception),
            "The specified dataset or workspace cannot be found, "
            "or the dataset doesn't have a user with the required permissions!",
        )

    def test_verify_last_refresh_success(self):
        # Arrange
        sut = PowerBi(PowerBiClient())
        sut.last_status = "Completed"
        sut.last_refresh_utc = datetime(2024, 2, 1, 15, 1)
        sut.min_refresh_time_utc = datetime(2024, 2, 1, 15, 0)
        sut.max_minutes_after_last_refresh = 2

        # Act
        result = sut._verify_last_refresh()

        # Assert
        self.assertTrue(result)

    def test_verify_last_refresh_failure(self):
        # Arrange
        sut = PowerBi(PowerBiClient())
        sut.last_status = "Completed"
        sut.last_refresh_utc = datetime(2024, 2, 1, 15, 1)
        sut.min_refresh_time_utc = datetime(2024, 2, 1, 15, 5)
        sut.max_minutes_after_last_refresh = 2

        # Act
        with self.assertRaises(Exception) as context:
            sut._verify_last_refresh()

        # Assert
        self.assertIn("Last refresh finished more than", str(context.exception))

 @patch('requests.post')
    def test_trigger_new_refresh_success(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 202
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient())
        sut.last_status = "Completed"
        sut.powerbi_url = "abc"
        sut.workspace_id = "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0"
        sut.dataset_id = "b1f0a07e-e348-402c-a2b2-11f3e31181ce"

        # Act
        result = sut._trigger_new_refresh()

        # Assert
        self.assertTrue(result)

    @patch('requests.post')
    def test_trigger_new_refresh_failure(self, mock_get):
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "error"
        mock_get.return_value = mock_response

        sut = PowerBi(PowerBiClient())
        sut.last_status = "Completed"
        sut.powerbi_url = "abc"
        sut.workspace_id = "614850c2-3a5c-4d2d-bcaa-d3f20f32a2e0"
        sut.dataset_id = "b1f0a07e-e348-402c-a2b2-11f3e31181ce"

        # Act
        with self.assertRaises(Exception) as context:
            sut._trigger_new_refresh()

        # Assert
        self.assertIn("Failed to trigger a refresh!", str(context.exception))

    def test_get_seconds_to_wait_first(self):
        sut = PowerBi(PowerBiClient())
        sut.last_duration = 0
        elapsed = 5

        # Call the method you want to test
        result = sut._get_seconds_to_wait(elapsed)

        # Assertions
        self.assertEqual(result, 15)

    def test_get_seconds_to_wait_next(self):
        sut = PowerBi(PowerBiClient())
        sut.last_duration = 60 * 15
        elapsed = 5

        # Call the method you want to test
        result = sut._get_seconds_to_wait(elapsed)

        # Assertions
        self.assertEqual(result, 60 * 5)
