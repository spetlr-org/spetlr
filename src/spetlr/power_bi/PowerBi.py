import time
from datetime import datetime, timedelta

import msal
import pandas as pd
import requests
from dateutil import parser
from pytz import timezone, utc

from spetlr.exceptions import SpetlrException

from .PowerBiClient import PowerBiClient


class PowerBi:
    def __init__(
        self,
        client: PowerBiClient,
        *,
        workspace_id: str = None,
        workspace_name: str = None,
        dataset_id: str = None,
        dataset_name: str = None,
        max_minutes_after_last_refresh: int = 12 * 60,
        timeout_in_seconds: int = 15 * 60,
        number_of_retries: int = 0,
        local_timezone_name: str = "UTC",
        ignore_errors: bool = False,
    ):
        """
        Allows refreshing PowerBI datasets and checking if the last refresh
            completed successfully.
        If no workspace is specified, a list of available workspaces will be displayed.
        If no dataset is specified, a list of available datasets in the given workspace
            will be displayed.

        :param PowerBiClient client: PowerBI client credentials.

        :param str workspace_id: The GUID of the workspace.
        :param str workspace_name: The name of the workspace
            (specified instead of the workspace_id).
        :param str dataset_id: The GUID of the dataset.
        :param str dataset_name: The name of the dataset
            (specified instead of the dataset_id).
        :param int max_minutes_after_last_refresh: The number of minutes
            for which the last succeeded refresh is considered valid,
            or 0 to disable time checking. Default is 12 hours.
        :param bool timeout_in_seconds: The number of seconds after which
            the refresh() method times out. Default is 15 minutes.
        :param int number_of_retries: The number of retries on transient
            errors when calling refresh(). Default is 0 (no retries).
            (e.g. 1 means two attempts in total.)
            Used only when the timeout_in_seconds parameter allows it!
        :param str local_timezone_name: The timezone to use when showing
            refresh timestamps. Only used for printing timestamps.
            Default timezone is UTC.
        :param bool ignore_errors: True to print errors in the output
            or False (default) to cast a SpetlrException.
        """

        if workspace_id is not None and workspace_name is not None:
            raise ValueError("Specify either 'workspace_id' or 'workspace_name'!")
        if dataset_id is not None and dataset_name is not None:
            raise ValueError("Specify either 'dataset_id' or 'dataset_name'!")
        if max_minutes_after_last_refresh is None or max_minutes_after_last_refresh < 0:
            raise ValueError(
                "The 'max_minutes_after_last_refresh' parameter "
                "must be greater than or equal zero!"
            )
        if timeout_in_seconds is None or timeout_in_seconds <= 0:
            raise ValueError(
                "The 'timeout_in_seconds' parameter must be greater than zero!"
            )
        if number_of_retries is None or number_of_retries < 0:
            raise ValueError(
                "The 'number_of_retries' parameter "
                "must be greater than or equal zero!"
            )

        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name

        # Set access parameters
        self.client = client

        self.max_minutes_after_last_refresh = max_minutes_after_last_refresh
        self.timeout_in_seconds = timeout_in_seconds
        self.number_of_retries = number_of_retries
        self.local_timezone_name = (
            local_timezone_name if local_timezone_name is not None else "UTC"
        )
        self.ignore_errors = ignore_errors
        self.api_header = None
        self.expire_time = 0

        self.last_status = None
        self.last_exception = None
        self.last_refresh_utc = None
        self.last_duration_in_seconds = 0

    def _raise_error(self, message: str) -> None:
        if self.ignore_errors:
            print(message)
        else:
            raise SpetlrException(message)

    def _raise_api_error(self, message: str, api_call: requests.Response) -> None:
        print(api_call.text)
        self._raise_error(
            message + f" Response: {api_call.status_code} {api_call.reason}"
        )

    def _get_access_token(self) -> bool:
        """
        Acquires an access token to connect to PowerBI.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if self.expire_time != 0:
            if time.time() < self.expire_time:
                return True
            print("Renewing access token...")

        # Prepare URLs
        authority_url = f"https://login.microsoftonline.com/{self.client.tenant_id}/"
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        self.powerbi_url = "https://api.powerbi.com/v1.0/myorg/"

        # Use MSAL to get an access token
        app = msal.ConfidentialClientApplication(
            self.client.client_id,
            authority=authority_url,
            client_credential=self.client.client_secret,
        )
        result = app.acquire_token_for_client(scopes=scope)
        if "access_token" not in result:
            print(result)
            self._raise_error("Failed to acquire token!")
            return False

        default_expires_in = 5 * 60
        extra_seconds = 3

        access_token = result["access_token"]
        if "expires_in" in result:
            self.expire_time = time.time() + result["expires_in"] - extra_seconds
        else:
            self.expire_time = time.time() + default_expires_in - extra_seconds

        self.api_header = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }
        return True

    @staticmethod
    def _show_workspaces(df: pd.DataFrame) -> None:
        print("Available workspaces:")
        df.rename(
            columns={"id": "workspace_id", "name": "workspace_name"},
            inplace=True,
        )
        df.display()

    @staticmethod
    def _show_datasets(df: pd.DataFrame) -> None:
        print("Available datasets:")
        df.rename(
            columns={"id": "dataset_id", "name": "dataset_name"},
            inplace=True,
        )
        df.display()

    def _get_workspace(self) -> bool:
        """
        Gets workspace ID based on the workspace name, or shows all workspaces
            if no parameter was specified.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if self.workspace_id is None:
            api_call = requests.get(
                url=f"{self.powerbi_url}groups", headers=self.api_header
            )
            if api_call.status_code != 200:
                self._raise_api_error("Failed to fetch workspaces!", api_call)
                return False
            df = pd.DataFrame(api_call.json()["value"], columns=["id", "name"])

            if self.workspace_name is None:
                self._show_workspaces(df)
                return False

            rows = df.loc[df["name"] == self.workspace_name, "id"]
            if rows.empty:
                self._raise_error(
                    f"Workspace name '{self.workspace_name}' cannot be found!"
                )
                return False
            self.workspace_id = rows.values[0]

        return True

    def _get_dataset(self) -> bool:
        """
        Gets dataset ID based on the dataset name, or shows all datasets
            if no parameter was specified.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if self.dataset_id is None:
            api_call = requests.get(
                url=f"{self.powerbi_url}groups/{self.workspace_id}/datasets",
                headers=self.api_header,
            )
            if api_call.status_code != 200:
                self._raise_api_error("Failed to fetch datasets!", api_call)
                return False
            df = pd.DataFrame(api_call.json()["value"], columns=["id", "name"])

            if self.dataset_name is None:
                self._show_datasets(df)
                return False

            rows = df.loc[df["name"] == self.dataset_name, "id"]
            if rows.empty:
                self._raise_error(
                    f"Dataset name '{self.dataset_name}' cannot be found, "
                    "or the dataset doesn't have a user with the required permissions!"
                )
                return False
            self.dataset_id = rows.values[0]

        return True

    def _connect(self) -> bool:
        """
        Connects or reconnects to PowerBI to fetch refresh history or trigger a refresh.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if not self._get_access_token():
            return False
        if not self._get_workspace():
            return False
        if not self._get_dataset():
            return False

        return True

    def _get_last_refresh(self) -> bool:
        """
        Gets the latest record in the PowerBI dataset refresh history.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if not self._connect():
            return False

        self.last_status = None
        self.last_exception = None
        self.last_refresh_utc = None
        self.last_duration_in_seconds = 0

        # Note: we fetch only the latest refresh record, i.e. top=1
        api_url = (
            f"{self.powerbi_url}groups/{self.workspace_id}"
            f"/datasets/{self.dataset_id}/refreshes?$top=1"
        )
        api_call = requests.get(url=api_url, headers=self.api_header)
        if api_call.status_code == 200:
            json = api_call.json()
            df = pd.DataFrame(
                json["value"],
                columns=[
                    "requestId",
                    "id",
                    "refreshType",
                    "startTime",
                    "endTime",
                    "status",
                    "serviceExceptionJson",
                ],
            )
            if not df.empty:
                df.set_index("id")
                self.last_status = df.status[0]
                self.last_exception = df.serviceExceptionJson[0]
                start_time = df.startTime[0]
                end_time = df.endTime[0]
                if (
                    self.last_status == "Completed"
                    and end_time is not None
                    and len(end_time) > 0
                ):
                    self.last_refresh_utc = parser.parse(end_time).replace(tzinfo=utc)
                    if start_time is not None and len(start_time) > 0:
                        self.last_duration_in_seconds = int(
                            (
                                parser.parse(end_time) - parser.parse(start_time)
                            ).total_seconds()
                        )
            return True
        elif api_call.status_code == 404:
            self._raise_error(
                "The specified dataset or workspace cannot be found, "
                "or the dataset doesn't have a user with the required permissions!"
            )
        else:
            self._raise_api_error("Failed to fetch refresh history!", api_call)
        return False

    def _verify_last_refresh(self) -> bool:
        """
        Checks if the last refresh of the PowerBI dataset completed successfully,
            and verifies if it happened recently enough.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if self.last_status is None:
            self._raise_error("Refresh is still in progress or never triggered!")
        elif self.last_status == "Completed":
            if self.last_refresh_utc is None:
                self._raise_error("Completed at unknown refresh time!")
            else:
                last_refresh_str = self.last_refresh_utc.astimezone(
                    timezone(self.local_timezone_name)
                ).strftime("%Y-%m-%d %H:%M") + (
                    " (UTC)"
                    if self.local_timezone_name.lower() == "utc"
                    else " (local time)"
                )
                min_refresh_time_utc = datetime.now(utc) - timedelta(
                    minutes=self.max_minutes_after_last_refresh
                )
                if (
                    self.max_minutes_after_last_refresh > 0
                    and self.last_refresh_utc < min_refresh_time_utc
                ):
                    self._raise_error(
                        "Last refresh finished more than "
                        f"{self.max_minutes_after_last_refresh} "
                        f"minutes ago at {last_refresh_str} !"
                    )
                else:
                    print(f"Refresh completed successfully at {last_refresh_str}.")
                    return True
        elif self.last_status == "Unknown":
            self._raise_error("Refresh is still in progress!")
        elif self.last_status == "Disabled":
            self._raise_error("Refresh is disabled!")
        elif self.last_status == "Failed":
            self._raise_error(f"Last refresh failed! {self.last_exception}")
        else:
            self._raise_error(
                f"Unknown refresh status: {self.last_status}! {self.last_exception}"
            )
        return False

    def _trigger_new_refresh(self) -> bool:
        """
        Starts a refresh of the PowerBI dataset.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        if self.last_status is None or self.last_status in ["Completed", "Failed"]:
            if self.last_status == "Failed":
                print(f"Warning: Last refresh failed! {self.last_exception}")
                print()

            api_url = (
                f"{self.powerbi_url}groups/{self.workspace_id}"
                f"/datasets/{self.dataset_id}/refreshes"
            )
            api_call = requests.post(url=api_url, headers=self.api_header)
            if api_call.status_code == 202:
                print("A new refresh has been successfully triggered.")
                return True
            else:
                self._raise_api_error("Failed to trigger a refresh!", api_call)
        elif self.last_status == "Unknown":
            print("Refresh is already in progress!")
            return True
        elif self.last_status == "Disabled":
            self._raise_error("Refresh is disabled!")
        else:
            self._raise_error(
                f"Unknown refresh status: {self.last_status}! {self.last_exception}"
            )
        return False

    def _get_seconds_to_wait(self, elapsed_seconds: int) -> int:
        """
        Returns the number of seconds to wait before rechecking
            if the refresh has completed. The method makes sure as few requests
            to the PowerBI API as possible would be made.

        :return: number of seconds to wait
        :rtype: int
        """

        # Microsoft set a limit of how many API requests in an hour can be made
        # to the PowerBI API. This logic is a fine compromise between how often
        # (small delay) we check through polling if the refresh has finished and
        # how soon (large delay) will the refresh() method complete.

        remaining_seconds = self.timeout_in_seconds - elapsed_seconds

        wait_seconds = elapsed_seconds
        if self.last_duration_in_seconds > 0:
            wait_seconds = abs(self.last_duration_in_seconds - elapsed_seconds)

        wait_seconds = (
            15 if wait_seconds < 40 else 60 if wait_seconds < (3 * 60) else (5 * 60)
        )

        return (
            remaining_seconds
            if (0 < remaining_seconds < wait_seconds)
            else wait_seconds
        )

    def check(self) -> bool:
        """
        Checks if the last refresh of the PowerBI dataset completed successfully,
            and verifies if it happened recently enough.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        return self._get_last_refresh() and self._verify_last_refresh()

    def start_refresh(self) -> bool:
        """
        Starts a refresh of the PowerBI dataset without waiting.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        return self._get_last_refresh() and self._trigger_new_refresh()

    def refresh(self) -> bool:
        """
        Starts a refresh of the PowerBI dataset and waits until completed.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises SpetlrException: if failed and ignore_errors==False
        """

        retries = self.number_of_retries
        start_time = time.time()
        if not self.start_refresh():
            return False

        while True:
            elapsed_seconds = int(time.time() - start_time)
            if elapsed_seconds >= self.timeout_in_seconds:
                print("Timeout!")
                break
            wait_seconds = self._get_seconds_to_wait(elapsed_seconds)
            print(f"Waiting {wait_seconds} seconds...")
            time.sleep(wait_seconds)

            if not self._get_last_refresh():
                return False

            if self.last_status == "Failed" and retries > 0:
                retries = retries - 1
                if not self._trigger_new_refresh():
                    return False
                continue

            if self.last_status != "Unknown":
                break

        return self._verify_last_refresh()
