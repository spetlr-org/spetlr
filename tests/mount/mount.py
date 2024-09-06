from spetlr.functions import init_dbutils


def mount_storage_account():
    """
    Mounts Azure storage account to Databricks cluster.

    Returns:
    None
    """
    # TODO: Remove when UC is enabled with volumes pointing to the Azure storage.

    client_id = init_dbutils().secrets.get(scope="secrets", key="Databricks--ClientId")
    client_secret = init_dbutils().secrets.get(
        scope="secrets", key="Databricks--ClientSecret"
    )
    tenant_id = init_dbutils().secrets.get(scope="secrets", key="Databricks--TenantId")
    resource_name = init_dbutils().secrets.get(scope="values", key="resourceName")

    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    source = f"abfss://silver@{resource_name}.dfs.core.windows.net/"
    mount_point = f"/mnt/{resource_name}/silver/"

    for mount in init_dbutils().fs.mounts():
        if mount.mountPoint == mount_point:
            init_dbutils().fs.unmount(mount_point)
            break

    init_dbutils().fs.mount(
        source=source, mount_point=mount_point, extra_configs=configs
    )
