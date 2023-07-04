"""Simplified access to the databricks api."""

from databricks.sdk import WorkspaceClient

import spetlr


def getDbApi() -> WorkspaceClient:
    """
    This method automatically configures a databricks API client.
    In local running, the databricks-cli is used for configuration.
    Running on a cluster, the configuration is extracted from the job context.
    """
    # Since the introduction of databricks-sdk this function is basically obsolete
    # This also closes #60
    return WorkspaceClient(product="spetlr", product_version=spetlr.__version__)
