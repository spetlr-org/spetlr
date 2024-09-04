# Databricks notebook source
client_id = dbutils.secrets.get(scope="secrets", key="Databricks--ClientId")
client_secret = dbutils.secrets.get(scope="secrets", key="Databricks--ClientSecret")
tenant_id = dbutils.secrets.get(scope="secrets", key="Databricks--TenantId")
resource_name = dbutils.secrets.get(scope="values", key="resourceName")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

source = f"abfss://silver@{resource_name}.dfs.core.windows.net/"
mount_point = f"/mnt/{resource_name}/silver/"

# COMMAND ----------

for mount in dbutils.fs.mounts():
    if mount.mountPoint == mount_point:
        dbutils.fs.unmount(mount_point)
    break

# COMMAND ----------

dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
