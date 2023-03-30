import base64
import hashlib
import hmac
import json
import warnings
from datetime import date, datetime
from typing import Any, Dict

import requests
from pyspark.sql import DataFrame


class AzureLogAnalyticsHandle:
    """
    A handler class for interaction with Azure Log Analytics through the HTTP Data
    Collector API.

    This class is designed to facilitate reading from and writing to Azure Log Analytics
    from Databricks, using the specified Workspace ID and Shared Key for authentication.
    Log type defaults to 'DatabricksLoggingOrchestrator' but can be customized during
    initialization.

    For more details on the Azure Log Analytics HTTP Data Collector API, refer to:
    https://learn.microsoft.com/en-us/rest/api/loganalytics/create-request

    Attributes:
    ----------
    workspace_id : str
        The unique identifier of the Azure Log Analytics workspace.

    shared_key : str
        The shared key for the Azure Log Analytics workspace. This key is used for
        authentication when sending data to the HTTP Data Collector API.

    log_type : str, optional (default = "DatabricksLogAnalyticsHandle")
        The type of log to be written to Azure Log Analytics.

    Methods:
    -------
    append(df: DataFrame) -> None:
        Posts a DataFrame to the Azure Log Analytics workspace using the HTTP Data
        Collector API.
        If the request is not successful, a warning is raised with the response code.

    read() -> DataFrame:
        Retrieves data from the Azure Log Analytics workspace.
        This method is currently not implemented.
    """

    def __init__(
        self,
        log_analytics_workspace_id: str,
        shared_key: str,
        log_type: str = "Databricks",
    ):
        self.workspace_id = log_analytics_workspace_id
        self.shared_key = shared_key
        self.log_type = log_type

    def _create_uri(self, resource: str) -> str:
        return (
            f"https://{self.workspace_id}.ods.opinsights"
            + f".azure.com{resource}?api-version=2016-04-01"
        )

    def _create_body(self, df: DataFrame) -> str:
        body = df.collect()
        body = [row.asDict() for row in body]
        body = json.dumps(body, indent=4, default=self._json_serialize)

        return body

    def _create_headers(
        self, method: str, content_type: str, content_length: int, resource: str
    ) -> Dict[str, str]:
        date_rfc1123_format = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

        x_headers = f"x-ms-date:{date_rfc1123_format}"
        string_to_hash = (
            f"{method}\n{str(content_length)}\n{content_type}\n{x_headers}\n{resource}"
        )
        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode()
        authorization = f"SharedKey {self.workspace_id}:{encoded_hash}"

        return {
            "content-type": content_type,
            "Authorization": authorization,
            "Log-Type": self.log_type,
            "x-ms-date": date_rfc1123_format,
        }

    def api_post(self, df: DataFrame) -> None:
        resource = "/api/logs"

        uri = self._create_uri(resource=resource)
        body = self._create_body(df)
        headers = self._create_headers(
            method="POST",
            content_type="application/json",
            content_length=len(body),
            resource=resource,
        )

        response = requests.post(uri, data=body, headers=headers)

        status_code = response.status_code

        if status_code != 200:
            warnings.warn(
                "Failure to send message to Azure Log Workspace. "
                + f"Response code: {status_code}"
            )
        else:
            print(f"Logging API POST method response code: {status_code}")

        return status_code

    append = api_post  # an alias for the post method

    def api_get(self) -> DataFrame:
        """For more details on the Azure Log Analytics Query API that can be used for
        the GET method, refer to:
        https://learn.microsoft.com/en-us/rest/api/loganalytics/dataaccess/query/get?tabs=HTTP
        """
        raise NotImplementedError("This has not been implemented yet")

    read = api_get

    @staticmethod
    def _json_serialize(obj: Any) -> str:
        # helper function to deal with casting timestamps and dates to json
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))
