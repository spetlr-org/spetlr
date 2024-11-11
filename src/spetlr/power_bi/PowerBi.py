import time
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Tuple

import msal
import pandas as pd
import requests
from pyspark.sql import DataFrame
from pytz import utc

from spetlr.power_bi.PowerBiClient import PowerBiClient
from spetlr.power_bi.PowerBiException import PowerBiException
from spetlr.power_bi.SparkPandasDataFrame import SparkPandasDataFrame


class PowerBi:
    def __init__(
        self,
        client: PowerBiClient,
        *,
        workspace_id: str = None,
        workspace_name: str = None,
        dataset_id: str = None,
        dataset_name: str = None,
        table_names: List[str] = None,
        max_minutes_after_last_refresh: int = 12 * 60,
        timeout_in_seconds: int = 15 * 60,
        number_of_retries: int = 0,
        max_parallelism: int = None,
        apply_refresh_policy: bool = None,
        dataset_refresh_type: str = None,
        dataset_commit_mode: str = None,
        incremental_effective_date: str = None,
        mail_on_failure: bool = False,
        mail_on_completion: bool = False,
        exclude_creators: List[str] = None,
        local_timezone_name: str = None,
        ignore_errors: bool = False,
    ):
        """
        Allows refreshing PowerBI datasets and checking if the last refresh
            completed successfully.
        If no workspace is specified, a list of available workspaces will be displayed.
        If no dataset is specified, a list of available datasets in the given workspace
            will be displayed.
        if the table names are an empty empty list, a list of available tables will
            be displayed.

        :param PowerBiClient client: PowerBI client credentials.

        :param str workspace_id: The GUID of the workspace.
        :param str workspace_name: The name of the workspace
            (specified instead of the workspace_id).
        :param str dataset_id: The GUID of the dataset.
        :param str dataset_name: The name of the dataset
            (specified instead of the dataset_id).
        :param list[str] table_names: Optional list of table names to be
            refreshed. By default all tables are refreshed and checked.
        :param int max_minutes_after_last_refresh: The number of minutes
            for which the last succeeded refresh is considered valid,
            or 0 to disable time checking. Default is 12 hours.
        :param bool timeout_in_seconds: The number of seconds after which
            the refresh() method times out. Default is 15 minutes.
        :param int number_of_retries: The number of retries on transient
            errors when calling refresh() and start_refresh().
            Default is 0 (no retries). (E.g. 1 means two attempts in total.)
            Used only when the timeout_in_seconds parameter allows it!
        :param int max_parallelism: Specifies the maximum number of threads
            that can run processing commands in parallel during a refresh
            in PowerBI. Default is None, which corresponds to 10 threads.
            Used only with the refresh() and start_refresh() methods!
        :param bool apply_refresh_policy: Determine if the refresh policy is applied
            or not (True or False). Default is None.
        :param str dataset_refresh_type: The type of processing to perform
            when refreshing the dataset
            ("Automatic", "Calculate", "ClearValues", "DataOnly", "Defragment", "Full").
            Default is None.
        :param str dataset_commit_mode: Determines if dataset objects will be
            committed in batches or only when complete
            ("PartialBatch", "Transactional"). Default is None.
        :param str incremental_effective_date: Effective date for incremental load.
            If an incremental refresh policy is applied, the effectiveDate parameter
            overrides the current date. Default is None.
        :param str local_timezone_name: The timezone to use when parsing
            timestamp columns. The default timezone is UTC.
            If the timezone is UTC, all timestamp columns will have a suffix "Utc".
            Otherwise they will have a suffix "Local".
        :param bool mail_on_failure: True to send an e-mail to the dataset
            owner when the refresh fails. Does not work with service principals!
        :param bool mail_on_completion: True to send an e-mail to the dataset
            owner when the refresh completes. Does not work with service principals!
        :param list[str] exclude_creators: When getting refresh histories/tables
            from several datasets simultaneously, exclude datasets configured
            by the specified creators (list of e-mails).
            This is to prevent the "Skipping unauthorized" message.
        :param bool ignore_errors: True to print errors in the output
            or False (default) to cast a PowerBiException.
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
        if max_parallelism is not None and max_parallelism < 0:
            raise ValueError(
                "The 'max_parallelism' parameter must be greater than or equal zero!"
            )

        if (mail_on_failure or mail_on_completion) and table_names is not None:
            raise ValueError(
                "The options for refreshing selected tables "
                "and for sending notification emails cannot be combined!"
            )

        self.client = client
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.table_names = table_names
        self.max_minutes_after_last_refresh = max_minutes_after_last_refresh
        self.timeout_in_seconds = timeout_in_seconds
        self.number_of_retries = number_of_retries
        self.max_parallelism = max_parallelism
        self.mail_on_failure = mail_on_failure
        self.mail_on_completion = mail_on_completion
        self.apply_refresh_policy = apply_refresh_policy
        self.dataset_refresh_type = dataset_refresh_type
        self.dataset_commit_mode = dataset_commit_mode
        self.incremental_effective_date = incremental_effective_date
        self.exclude_creators = (
            [(creator.lower() if creator else None) for creator in exclude_creators]
            if exclude_creators
            else []
        )
        self.local_timezone_name = local_timezone_name
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
            raise PowerBiException(message)

    def _raise_base_api_error(
        self,
        message: str,
        api_call: requests.Response,
        *,
        post_body: str = None,
        additional_message: str = None,
    ) -> None:
        print(api_call.text)
        print(api_call.url)
        if post_body is not None:
            print(post_body)
        if additional_message is not None:
            print(additional_message)
        self._raise_error(
            message + f" Response: {api_call.status_code} {api_call.reason}"
        )

    def _raise_api_error(
        self,
        message: str,
        api_call: requests.Response,
        *,
        post_body: str = None,
        workspace_id: str = None,
        dataset_id: str = None,
        request_id: str = None,
        ignore_unauthorized: bool = False,
    ) -> None:
        additional_message = ""
        if workspace_id is not None or dataset_id is not None or request_id is not None:
            additional_message = (
                f'workspace_id="{workspace_id}", dataset_id="{dataset_id}"'
            )
            if request_id is not None:
                additional_message += f', request_id="{request_id}"'
        if api_call.status_code == 404 and request_id is None:
            message = (
                "The specified dataset or workspace cannot be found, "
                "or the dataset doesn't have a user with the required permissions!"
            )
        if api_call.status_code == 401 and ignore_unauthorized:
            print("Skipping unauthorized: " + additional_message)
        else:
            self._raise_base_api_error(
                message,
                api_call,
                post_body=post_body,
                additional_message=additional_message,
            )

    def _get_access_token(self) -> bool:
        """
        Acquires an access token to connect to PowerBI.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
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

    def _get_workspaces(self) -> Optional[SparkPandasDataFrame]:
        """
        Returns the list of available PowerBI workspaces.

        :return: data frame with workspaces if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        api_call = requests.get(
            url=f"{self.powerbi_url}groups", headers=self.api_header
        )
        if api_call.status_code == 200:
            return SparkPandasDataFrame(
                api_call.json()["value"],
                [
                    ("name", "WorkspaceName", "string"),
                    ("isReadOnly", "IsReadOnly", "boolean"),
                    ("isOnDedicatedCapacity", "IsOnDedicatedCapacity", "boolean"),
                    ("capacityId", "CapacityId", "string"),
                    (
                        "defaultDatasetStorageFormat",
                        "DefaultDatasetStorageFormat",
                        "string",
                    ),
                    ("dataflowStorageId", "DataflowStorageId", "string"),
                    ("id", "WorkspaceId", "string"),
                ],
                indexing_columns="id",
                sorting_columns="name",
            )
        self._raise_base_api_error("Failed to fetch workspaces!", api_call)
        return None

    def _get_datasets(self, workspace_id: str) -> Optional[SparkPandasDataFrame]:
        """
        Returns the list of available PowerBI datasets.

        :param str workspace_id: the id of the workspace to fetch
        :return: data frame with datasets if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        api_call = requests.get(
            url=f"{self.powerbi_url}groups/{workspace_id}/datasets",
            headers=self.api_header,
        )
        if api_call.status_code == 200:
            return SparkPandasDataFrame(
                api_call.json()["value"],
                [
                    ("name", "DatasetName", "string"),
                    ("configuredBy", "ConfiguredBy", "string"),
                    ("isRefreshable", "IsRefreshable", "boolean"),
                    ("addRowsAPIEnabled", "AddRowsApiEnabled", "boolean"),
                    (
                        "isEffectiveIdentityRequired",
                        "IsEffectiveIdentityRequired",
                        "boolean",
                    ),
                    (
                        "isEffectiveIdentityRolesRequired",
                        "IsEffectiveIdentityRolesRequired",
                        "boolean",
                    ),
                    ("isOnPremGatewayRequired", "IsOnPremGatewayRequired", "boolean"),
                    ("id", "DatasetId", "string"),
                ],
                indexing_columns="id",
                sorting_columns=["name", "configuredBy"],
            )

        self._raise_base_api_error("Failed to fetch datasets!", api_call)
        return None

    def _get_refresh_history(
        self,
        workspace_id: str,
        dataset_id: str,
        ignore_unauthorized: bool = False,
    ) -> Optional[SparkPandasDataFrame]:
        """
        Returns the PowerBI dataset refresh history for the specified dataset.

        :param str workspace_id: the id of the workspace
        :param str dataset_id: the id of the dataset
        :param bool ignore_unauthorized: return None on HTTP 401 Unauthorized
        :return: data frame with the refresh history if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        api_url = (
            f"{self.powerbi_url}groups/{workspace_id}/datasets/{dataset_id}/refreshes"
        )
        schema = [
            ("refreshType", "RefreshType", "string"),
            ("status", "Status", "string"),
            (
                lambda df: (df["endTime"] - df["startTime"]) / pd.Timedelta(seconds=1),
                "Seconds",
                "long",
            ),
            ("startTime", "StartTime", "timestamp"),
            ("endTime", "EndTime", "timestamp"),
            ("serviceExceptionJson", "Error", "string"),
            ("requestId", "RequestId", "string"),
            ("id", "RefreshId", "long"),
            ("refreshAttempts", "RefreshAttempts", "string"),
        ]
        api_call = requests.get(url=api_url, headers=self.api_header)
        if api_call.status_code == 200:
            return SparkPandasDataFrame(
                api_call.json()["value"],
                schema,
                indexing_columns="id",
                local_timezone_name=self.local_timezone_name,
            )
        self._raise_api_error(
            "Failed to fetch refresh history!",
            api_call,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            ignore_unauthorized=ignore_unauthorized,
        )
        return None

    def _get_refresh_history_details(
        self,
        workspace_id: str,
        dataset_id: str,
        request_id: str,
        ignore_unauthorized: bool = False,
    ) -> Optional[SparkPandasDataFrame]:
        """
        Returns the PowerBI dataset refresh history details for the specified refresh.

        :param str workspace_id: the id of the workspace
        :param str dataset_id: the id of the dataset
        :param str request_id: the id of the refresh request
        :param bool ignore_unauthorized: return None on HTTP 401 Unauthorized
        :return: data frame with the refresh history details if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        api_url = (
            f"{self.powerbi_url}groups/{workspace_id}"
            f"/datasets/{dataset_id}/refreshes/{request_id}"
        )
        schema = [
            ("table", "TableName", "string"),
            ("partition", "PartitionName", "string"),
            ("status", "Status", "string"),
        ]
        api_call = requests.get(url=api_url, headers=self.api_header)
        if api_call.status_code == 200:
            return SparkPandasDataFrame(
                api_call.json()["objects"],
                schema,
                indexing_columns=["table", "partition"],
                sorting_columns=["table", "partition"],
            )
        self._raise_api_error(
            "Failed to fetch refresh history details!",
            api_call,
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            request_id=request_id,
            ignore_unauthorized=ignore_unauthorized,
        )
        return None

    def _get_partition_tables(
        self,
        workspace_id: str,
        dataset_id: str,
        ignore_unauthorized: bool = False,
    ) -> Optional[SparkPandasDataFrame]:
        """
        Returns the PowerBI dataset partition info with table names
        and their refresh times.

        :param str workspace_id: the id of the workspace
        :param str dataset_id: the id of the dataset
        :param bool ignore_unauthorized: return None on HTTP 401 Unauthorized
        :return: data frame with the partition info if succeeded or None if failed
        (when ignore_errors==True)
        :rtype: Pandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        api_url = (
            f"{self.powerbi_url}groups/{workspace_id}"
            f"/datasets/{dataset_id}/executeQueries"
        )
        dax_query = {
            "queries": [{"query": "EVALUATE INFO.PARTITIONS()"}],
            "serializerSettings": {"includeNulls": True},
        }
        schema = [
            ("[Name]", "TableName", "string"),
            (
                "[RefreshedTime]",
                "RefreshTime",
                "timestamp",
            ),
            ("[ModifiedTime]", "ModifiedTime", "timestamp"),
            ("[Description]", "Description", "string"),
            ("[ErrorMessage]", "ErrorMessage", "string"),
            ("[ID]", "Id", "long"),
            ("[TableID]", "TableId", "long"),
            ("[DataSourceID]", "DataSourceId", "int"),
            ("[QueryDefinition]", "QueryDefinition", "string"),
            ("[State]", "State", "int"),
            ("[Type]", "Type", "int"),
            ("[PartitionStorageID]", "PartitionStorageId", "int"),
            ("[Mode]", "Mode", "int"),
            ("[DataView]", "DataView", "int"),
            ("[SystemFlags]", "SystemFlags", "int"),
            (
                "[RetainDataTillForceCalculate]",
                "RetainDataTillForceCalculate",
                "boolean",
            ),
            ("[RangeStart]", "RangeStart", "timestamp"),
            ("[RangeEnd]", "RangeEnd", "timestamp"),
            ("[RangeGranularity]", "RangeGranularity", "int"),
            ("[RefreshBookmark]", "RefreshBookmark", "string"),
            ("[QueryGroupID]", "QueryGroupId", "int"),
            ("[ExpressionSourceID]", "ExpressionSourceId", "int"),
            ("[MAttributes]", "MAttributes", "int"),
        ]
        api_call = requests.post(url=api_url, headers=self.api_header, json=dax_query)
        if api_call.status_code == 200:
            return SparkPandasDataFrame(
                api_call.json()["results"][0]["tables"][0]["rows"],
                schema,
                indexing_columns="TableId",
                sorting_columns="TableName",
                local_timezone_name=self.local_timezone_name,
            )
        self._raise_api_error(
            "Failed to fetch partition info!",
            api_call,
            post_body=str(dax_query),
            workspace_id=workspace_id,
            dataset_id=dataset_id,
            ignore_unauthorized=ignore_unauthorized,
        )
        return None

    def _verify_workspace(self, *, force_verify: bool = False) -> bool:
        """
        Gets workspace ID based on the workspace name, or shows all workspaces
            if no parameter was specified.

        :param bool force_verify: True if the workspace_id should be verified as well
        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self.workspace_id is None or force_verify:
            workspaces = self._get_workspaces()
            if workspaces is None:
                return False
            df = workspaces.get_pandas_df()
            if self.workspace_id is not None:
                if self.workspace_id not in df.WorkspaceId.values:
                    self._raise_error(
                        f"Workspace id '{self.workspace_id}' cannot be found!"
                    )
                    return False
                return True
            if self.workspace_name is None:
                workspaces.show(
                    "Available workspaces:",
                    "No workspaces found.",
                    filter_columns=[
                        ("WorkspaceId", "workspace_id"),
                        ("WorkspaceName", "workspace_name"),
                    ],
                )
                return False
            rows = df.loc[df.WorkspaceName == self.workspace_name, "WorkspaceId"]
            if rows.empty:
                self._raise_error(
                    f"Workspace name '{self.workspace_name}' cannot be found!"
                )
                return False
            self.workspace_id = rows.values[0]

        return True

    def _verify_dataset(self, *, force_verify: bool = False) -> bool:
        """
        Gets dataset ID based on the dataset name, or shows all datasets
            if no parameter was specified.

        :param bool force_verify: True if the dataset_id should be verified as well
        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self.dataset_id is None or force_verify:
            datasets = self._get_datasets(self.workspace_id)
            if datasets is None:
                return False
            df = datasets.get_pandas_df()
            if df.empty and not self._verify_workspace(force_verify=True):
                return False
            if self.dataset_id is not None:
                if self.dataset_id not in df.DatasetId.values:
                    self._raise_error(
                        f"Dataset id '{self.dataset_id}' cannot be found!"
                    )
                    return False
                return True
            if self.dataset_name is None:
                datasets.show(
                    "Available datasets:",
                    "No datasets found.",
                    filter_columns=[
                        ("DatasetId", "dataset_id"),
                        ("DatasetName", "dataset_name"),
                    ],
                )
                return False
            rows = df.loc[df.DatasetName == self.dataset_name, "DatasetId"]
            if rows.empty:
                self._raise_error(
                    f"Dataset name '{self.dataset_name}' cannot be found, "
                    "or the dataset doesn't have a user with the required permissions!"
                )
                return False
            self.dataset_id = rows.values[0]
            if self.table_names is not None and (
                not self.table_names or not isinstance(self.table_names, list)
            ):
                tables = self._get_partition_tables(self.workspace_id, self.dataset_id)
                if tables is not None:
                    tables.show(
                        "Available tables:",
                        "The dataset has not tables.",
                        filter_columns=[("TableName", "table_name")],
                    )
                return False

        return True

    def _connect(self) -> bool:
        """
        Connects or reconnects to the PowerBI API.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if not self._get_access_token():
            return False
        if not self._verify_workspace():
            return False
        if not self._verify_dataset():
            return False

        return True

    def _connect_and_get_workspaces(
        self,
        *,
        skip_read_only: bool = False,
    ) -> Optional[List[Tuple[str, str]]]:
        """
        Connects or reconnects to the PowerBI API and returns a list of
        workspace id's and names.

        :param bool skip_read_only: True to exclude read-only workspaces.
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if not self._get_access_token():
            return None
        if (
            self.workspace_id is not None
            or self.workspace_name is not None
            or self.dataset_id is not None
            or self.dataset_name is not None
        ):
            if not self._verify_workspace():
                return None
        if self.dataset_id is not None or self.dataset_name is not None:
            if not self._verify_dataset():
                return None
        workspace_list = [(self.workspace_id, self.workspace_name)]
        if self.workspace_id is None:
            workspaces = self._get_workspaces()
            if workspaces is None:
                return None
            workspace_list = [
                (df.WorkspaceId, df.WorkspaceName)
                for _, df in workspaces.get_pandas_df().iterrows()
                if not (skip_read_only and df.IsReadOnly)
            ]
        return workspace_list

    def _combine_datasets(self) -> Optional[SparkPandasDataFrame]:
        """
        Returns a list of PowerBI datasets for a single workspace or
        combined lists for datasets from all workspaces.

        :return: data frame with PowerBI datasets if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        workspace_list = self._connect_and_get_workspaces()
        if workspace_list is None:
            return None
        if self.workspace_id is not None:
            return self._get_datasets(self.workspace_id)
        result = None
        for workspace_id, workspace_name in workspace_list:
            datasets = self._get_datasets(workspace_id)
            if datasets is None:
                return None
            prefix_columns = [("WorkspaceName", workspace_name, "string")]
            suffix_columns = [("WorkspaceId", workspace_id, "string")]
            result = datasets.append(result, prefix_columns, suffix_columns)

        return result

    def _combine_dataframes(
        self,
        function: Callable[[str, str, bool], Optional[SparkPandasDataFrame]],
        *,
        skip_read_only: bool = False,
        skip_not_refreshable: bool = False,
        skip_effective_identity: bool = False,
    ) -> Optional[SparkPandasDataFrame]:
        """
        Returns a single data frame loaded from PowerBI, or the same
        combined data frame fetched from all workspaces and datasets.

        :param callable function: The function that returns the data frame
        :param bool skip_read_only: True to exclude read-only workspaces.
        :param bool skip_not_refreshable: True to exclude PowerBI datasets
           that are not refreshable.
        :param bool skip_effective_identity: True to exclude PowerBI datasets
            requiring an effective identity
            (effective identity is not supported in this version of spetlr).
        :return: requested data frame if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        workspace_list = self._connect_and_get_workspaces(skip_read_only=skip_read_only)
        if workspace_list is None:
            return None
        if self.workspace_id is not None and self.dataset_id is not None:
            return function(self.workspace_id, self.dataset_id)
        result = None
        for workspace_id, workspace_name in workspace_list:
            datasets = self._get_datasets(workspace_id)
            if datasets is None:
                return None
            for dataset_id, dataset_name in (
                (df.DatasetId, df.DatasetName)
                for _, df in datasets.get_pandas_df().iterrows()
                if not (skip_not_refreshable and not df.IsRefreshable)
                and not (
                    skip_effective_identity
                    and (
                        df.IsEffectiveIdentityRequired
                        or df.IsEffectiveIdentityRolesRequired
                    )
                )
                and (df.ConfiguredBy.lower() if df.ConfiguredBy else None)
                not in self.exclude_creators
            ):
                data = function(workspace_id, dataset_id, True)
                if data is None:
                    continue
                prefix_columns = [
                    ("WorkspaceName", workspace_name, "string"),
                    ("DatasetName", dataset_name, "string"),
                ]
                suffix_columns = [
                    ("WorkspaceId", workspace_id, "string"),
                    ("DatasetId", dataset_id, "string"),
                ]
                if self.workspace_id is not None:
                    prefix_columns = prefix_columns[1:]
                    suffix_columns = suffix_columns[1:]
                result = data.append(result, prefix_columns, suffix_columns)

        return result

    def _combine_refresh_history_details(self):
        """
        Returns a single data frame with refresh history details, or the same
        combined data frame fetched from all workspaces and datasets.
        Only "ViaEnhancedApi" and "Completed" requests are included
        (i.e. when the refresh was executed with the "table_names"
        parameter specified).

        :return: data frame with refresh history details if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: SparkPandas data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        history = self._combine_dataframes(
            self._get_refresh_history, skip_read_only=True, skip_not_refreshable=True
        )
        if history is None:
            return None
        result = None
        for workspace_id, workspace_name, dataset_id, dataset_name, request_id in (
            (
                self.workspace_id if self.workspace_id else df.WorkspaceId,
                self.workspace_name if self.workspace_id else df.WorkspaceName,
                self.dataset_id if self.dataset_id else df.DatasetId,
                self.dataset_name if self.dataset_id else df.DatasetName,
                df.RequestId,
            )
            for _, df in history.get_pandas_df().iterrows()
            if df.RefreshType == "ViaEnhancedApi" and df.Status == "Completed"
        ):
            data = self._get_refresh_history_details(
                workspace_id, dataset_id, request_id, True
            )
            if data is None:
                continue
            prefix_columns = [
                ("WorkspaceName", workspace_name, "string"),
                ("DatasetName", dataset_name, "string"),
            ]
            suffix_columns = [
                ("WorkspaceId", workspace_id, "string"),
                ("DatasetId", dataset_id, "string"),
                ("RequestId", request_id, "string"),
            ]
            if self.dataset_id is not None:
                prefix_columns = prefix_columns[2:]
                suffix_columns = suffix_columns[2:]
            elif self.workspace_id is not None:
                prefix_columns = prefix_columns[1:]
                suffix_columns = suffix_columns[1:]
            result = data.append(result, prefix_columns, suffix_columns)

        return result

    def _get_last_refresh(self, *, deep_check: bool = False) -> bool:
        """
        Gets the latest record in the PowerBI dataset refresh history.

        :param bool deep_check: Used only with check() - ignore history rows
            where refresh is in progress or the refresh type doesn't match
        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        self.last_status = None
        self.last_exception = None
        self.is_enhanced = False
        self.last_refresh_utc = None
        self.table_name = None
        if not self._connect():
            return False
        history = self._get_refresh_history(self.workspace_id, self.dataset_id)
        if history is None:
            return False
        df = history.get_pandas_df()
        if not df.empty:
            first = 0
            while deep_check and (
                df.RefreshType.iloc[first] == "ViaEnhancedApi"
                and df.shape[0] > first + 1
                and (
                    self.table_names is None
                    or (first == 0 and df.Status.iloc[0] == "Unknown")
                )
            ):
                first += 1
            self.last_status = df.Status.iloc[first]
            self.last_exception = df.Error.iloc[first]
            self.is_enhanced = df.RefreshType.iloc[first] == "ViaEnhancedApi"
            # calculate the average duration of all previous API refresh calls
            # when there were no table names specified
            mean = df.loc[
                (df.RefreshType == "ViaApi") & (df.Status == "Completed"),
                df.Seconds.name,
            ].mean()
            if pd.isna(mean) or self.table_names:
                self.last_duration_in_seconds = 0
            else:
                self.last_duration_in_seconds = int(mean)
            if self.last_status == "Completed":
                refresh_time = df["EndTime" + history.time_column_suffix].iloc[first]
                if self.table_names is not None:
                    tables = self._get_partition_tables(
                        self.workspace_id, self.dataset_id, ignore_unauthorized=True
                    )
                    if tables is not None:
                        df = tables.get_pandas_df()
                        df = df[
                            df.TableName.isin(self.table_names)
                            & df.ErrorMessage.notnull()
                        ]
                        if not df.empty:
                            self.last_status = "Failed"
                            self.last_exception = df.ErrorMessage.iloc[0]
                            self.table_name = df.TableName.iloc[0]
                            return True
                        else:
                            df = tables.get_pandas_df()
                            df = df[df.TableName.isin(self.table_names)]
                            if not df.empty:
                                column = "RefreshTime" + tables.time_column_suffix
                                idx = df[column].idxmin()
                                self.table_name = df.TableName.loc[idx]
                                refresh_time = df[column].loc[idx]

                self.last_refresh_utc = history.get_utc_time(refresh_time)
                self.last_refresh_str = history.get_local_time_str(refresh_time)

        return True

    def _verify_last_refresh(self) -> bool:
        """
        Checks if the last refresh of the PowerBI dataset completed successfully,
            and verifies if it happened recently enough.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self.last_status is None:
            print("No refresh was triggered yet.")
        elif self.last_status == "Completed":
            if self.last_refresh_utc is None:
                self._raise_error("Completed at unknown refresh time!")
            else:
                min_refresh_time_utc = datetime.now(utc) - timedelta(
                    minutes=self.max_minutes_after_last_refresh
                )
                if (
                    self.max_minutes_after_last_refresh > 0
                    and self.last_refresh_utc < min_refresh_time_utc
                ):
                    self._raise_error(
                        (
                            "Last refresh finished more than "
                            f"{self.max_minutes_after_last_refresh} "
                            f"minutes ago at {self.last_refresh_str} !"
                        )
                        if self.table_name is None
                        else (
                            f"Last refresh of the table '{self.table_name}' finished "
                            f"more than {self.max_minutes_after_last_refresh} "
                            f"minutes ago at {self.last_refresh_str} !"
                        )
                    )
                else:
                    print(
                        f"Refresh completed successfully at {self.last_refresh_str}."
                        if self.table_name is None
                        else (
                            f"Last refresh of '{self.table_name}' (the oldest in "
                            "the specified tables) completed successfully "
                            f"at {self.last_refresh_str}."
                        )
                    )
                    return True
        elif self.last_status == "Unknown":
            self._raise_error("Refresh is still in progress!")
        elif self.last_status in ["Canceled", "Cancelled"]:
            self._raise_error("Refresh has been canceled!")
        elif self.last_status == "Disabled":
            self._raise_error("Refresh is disabled!")
        elif self.last_status == "Failed":
            self._raise_error(
                f"Last refresh failed! {self.last_exception}"
                if self.table_name is None
                else f"Last refresh for the table '{self.table_name}' "
                f"failed! {self.last_exception}"
            )
        else:
            self._raise_error(
                f"Unknown refresh status: {self.last_status}! {self.last_exception}"
            )
        return False

    def _get_refresh_argument_json(
        self, with_wait: bool
    ) -> Optional[Dict[str, object]]:
        """
        Returns the HTTP body of the PowerBI refresh API call
        containing table names to refresh or other parameters if necessary.

        :param bool with_wait: True if we need to wait for the refresh to finish.
        :rtype: JSON object or None
        """

        result = {}
        if self.table_names:
            result = {
                "type": "full",
                "commitMode": "transactional",
                "objects": [{"table": table} for table in self.table_names],
                "applyRefreshPolicy": "false",
            }
        elif self.mail_on_failure or self.mail_on_completion:
            result["notifyOption"] = (
                "MailOnCompletion" if self.mail_on_completion else "MailOnFailure"
            )
        if self.number_of_retries > 0 and not with_wait:
            if self.table_names:
                result["retryCount"] = self.number_of_retries
            else:
                print(
                    "The 'number_of_retries' parameter is ignored in "
                    "start_refresh() if 'table_names' is not specified!"
                )
        if self.max_parallelism is not None:
            result["maxParallelism"] = self.max_parallelism
        if self.apply_refresh_policy is not None:
            result["applyRefreshPolicy"] = str(self.apply_refresh_policy).lower()
        if self.dataset_refresh_type is not None:
            result["type"] = self.dataset_refresh_type
        if self.dataset_commit_mode is not None:
            result["commitMode"] = self.dataset_commit_mode
        if self.incremental_effective_date is not None:
            result["effectiveDate"] = self.incremental_effective_date

        return result if result else None

    def _trigger_new_refresh(self, *, with_wait: bool = True) -> bool:
        """
        Starts a refresh of the PowerBI dataset.

        :param bool with_wait: True if we need to wait for the refresh to finish.
        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self.last_status == "Disabled":
            self._raise_error("Refresh is disabled!")
        elif self.last_status == "Unknown":
            if self.is_enhanced or not self.table_names:
                if with_wait:
                    print("Refresh is in progress, waiting until completed.")
                else:
                    self._raise_error("Refresh is already in progress!")
                return True
            print("Refresh of the whole dataset is in progress. Nothing to do.")
        else:
            if self.last_status == "Failed":
                print(f"Warning: Last refresh failed! {self.last_exception}")
                print()
            elif self.last_status is not None and self.last_status not in [
                "Completed",
                "Canceled",
                "Cancelled",
            ]:
                print(
                    f"Unknown refresh status: {self.last_status}! {self.last_exception}"
                )
                print()

            post_body = self._get_refresh_argument_json(with_wait)
            api_url = (
                f"{self.powerbi_url}groups/{self.workspace_id}"
                f"/datasets/{self.dataset_id}/refreshes"
            )
            api_call = requests.post(
                url=api_url, headers=self.api_header, json=post_body
            )
            if api_call.status_code == 202:
                print("A new refresh has been successfully triggered.")
                self.last_status = "Unknown"
                return True
            else:
                self._raise_api_error(
                    "Failed to trigger a refresh!",
                    api_call,
                    post_body=str(post_body),
                    workspace_id=self.workspace_id,
                    dataset_id=self.dataset_id,
                )

        return False

    def _get_seconds_to_wait(self, elapsed_seconds: int) -> int:
        """
        Returns the number of seconds to wait before rechecking
            if the refresh has completed. The method makes sure as few requests
            to the PowerBI API as possible would be made.

        :param int elapsed_seconds: The number of seconds elapsed so far.
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
        Checks if the last refresh of the PowerBI dataset or its selected tables
            completed successfully, and verifies if it happened recently enough.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        return self._get_last_refresh(deep_check=True) and self._verify_last_refresh()

    def start_refresh(self) -> bool:
        """
        Starts a refresh of the PowerBI dataset without waiting.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        return self._get_last_refresh() and self._trigger_new_refresh(with_wait=False)

    def refresh(self) -> bool:
        """
        Starts a refresh of the PowerBI dataset and waits until completed.

        :return: True if succeeded or False if failed (when ignore_errors==True)
        :rtype: bool
        :raises PowerBiException: if failed and ignore_errors==False
        """

        retries = self.number_of_retries
        start_time = time.time()
        if not self._get_last_refresh():
            return False
        restart = self.last_status == "Unknown" and self.is_enhanced
        if not self._trigger_new_refresh():
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

            if (self.last_status == "Failed" and retries > 0) or (
                self.last_status == "Completed" and restart
            ):
                if not restart:
                    retries = retries - 1
                restart = False
                if not self._trigger_new_refresh():
                    return False
                continue

            if self.last_status != "Unknown":
                break

        return self._verify_last_refresh()

    def show_history(self) -> None:
        """
        Displays the refresh history of a PowerBI dataset.

        :rtype: None
        :raises PowerBiException: if failed and ignore_errors==False
        """

        history = self._combine_dataframes(
            self._get_refresh_history, skip_read_only=True, skip_not_refreshable=True
        )
        if history is not None:
            history.show("Refresh history:", "The refresh history is empty.")

    def get_history(self) -> Optional[DataFrame]:
        """
        Returns the refresh history of a PowerBI dataset in a Spark data frame.

        :return: the data frame with the refresh history if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: Spark data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        history = self._combine_dataframes(
            self._get_refresh_history, skip_read_only=True, skip_not_refreshable=True
        )
        return history.get_spark_df() if history is not None else None

    def show_history_details(self) -> None:
        """
        Displays refresh history details of a PowerBI dataset.

        :rtype: None
        :raises PowerBiException: if failed and ignore_errors==False
        """

        details = self._combine_refresh_history_details()
        if details is not None:
            details.show("Refresh history details:", "The refresh history is empty.")

    def get_history_details(self) -> Optional[DataFrame]:
        """
        Returns refresh history details of a PowerBI dataset in a Spark data frame.

        :return: the data frame with the refresh history details if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: Spark data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        history = self._combine_refresh_history_details()
        return history.get_spark_df() if history is not None else None

    def show_tables(self) -> None:
        """
        Displays the tables of a PowerBI dataset.

        :rtype: None
        :raises PowerBiException: if failed and ignore_errors==False
        """

        tables = self._combine_dataframes(
            self._get_partition_tables, skip_effective_identity=True
        )
        if tables is not None:
            tables.show("Dataset tables:", "No dataset tables found.")

    def get_tables(self) -> Optional[DataFrame]:
        """
        Returns the tables of a PowerBI dataset as a Spark data frame.

        :return: the data frame with the partition tables if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: Spark data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        tables = self._combine_dataframes(
            self._get_partition_tables, skip_effective_identity=True
        )
        return tables.get_spark_df() if tables is not None else None

    def show_workspaces(self) -> None:
        """
        Displays the available workspaces.

        :rtype: None
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self._get_access_token():
            workspaces = self._get_workspaces()
            if workspaces is not None:
                workspaces.show("Workspaces:", "No workspaces found.")

    def get_workspaces(self) -> Optional[DataFrame]:
        """
        Returns the available workspaces as a Spark data frame.

        :return: the data frame with the workspaces if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: Spark data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        if self._get_access_token():
            workspaces = self._get_workspaces()
            return workspaces.get_spark_df() if workspaces is not None else None
        return None

    def show_datasets(self) -> None:
        """
        Displays the available datasets.

        :rtype: None
        :raises PowerBiException: if failed and ignore_errors==False
        """

        datasets = self._combine_datasets()
        if datasets is not None:
            datasets.show("Datasets:", "No datasets found.")

    def get_datasets(self) -> Optional[DataFrame]:
        """
        Returns the available datasets as a Spark data frame.

        :return: the data frame with the datasets if succeeded,
            or None if failed (when ignore_errors==True)
        :rtype: Spark data frame
        :raises PowerBiException: if failed and ignore_errors==False
        """

        datasets = self._combine_datasets()
        return datasets.get_spark_df() if datasets is not None else None
