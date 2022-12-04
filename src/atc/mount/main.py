import argparse
import json
from types import SimpleNamespace

from atc.exceptions import AtcException
from atc.functions import init_dbutils


def main():
    parser = argparse.ArgumentParser(description="atc-dataplatform mountpoint setup.")
    parser.add_argument(
        "path",
        nargs="+",
        type=str,
        help="The json file to read the configuration from.\n"
        """Example json contents:
[
  {
    "storageAccountName":"atc",
    "secretScope":"atc",
    "clientIdName":"Databricks--ClientId",
    "clientSecretName":"Databricks--ClientSecret",
    "tenantIdName":"Databricks--TenantId",
    "containers":["silver"]
  }
]
        """,
    )
    args = parser.parse_args()

    required_keys = [
        "storageAccountName",
        "secretScope",
        "clientIdName",
        "clientSecretName",
        "tenantIdName",
        "containers",
    ]
    configs = []
    for path in args.path:
        with open(path) as json_file:
            for item in json.load(json_file):
                for key in required_keys:
                    if key not in item:
                        raise AtcException(f"{path} contains invalid shape.")
                configs.append(SimpleNamespace(**item))

    dbutils = init_dbutils()

    # Mounting
    all_mounts = [m[0] for m in dbutils.fs.mounts()]

    for item in configs:
        scope = item.secretScope

        clientId = dbutils.secrets.get(scope=scope, key=item.clientIdName)
        # ServicePrincipal KEY
        clientSecret = dbutils.secrets.get(scope=scope, key=item.clientSecretName)
        tenantId = dbutils.secrets.get(scope=scope, key=item.tenantIdName)

        prfx = "fs.azure.account."
        configs = {
            f"{prfx}auth.type": "OAuth",
            f"{prfx}oauth.provider.type": (
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
            ),
            f"{prfx}oauth2.client.id": clientId,
            f"{prfx}oauth2.client.secret": clientSecret,
            f"{prfx}oauth2.client.endpoint": (
                f"https://login.microsoftonline.com/{tenantId}/oauth2/token"
            ),
        }

        storageAccountUrl = item.storageAccountName + ".dfs.core.windows.net/"

        # Mount each container from the storage accounts
        for container in item.containers:
            source = f"abfss://{container}@" + storageAccountUrl
            mount_name = f"/mnt/{item.storageAccountName}/{container}/"

            if mount_name not in all_mounts:
                print(f"Mounting {mount_name}")
                dbutils.fs.mount(
                    source=source, mount_point=mount_name, extra_configs=configs
                )
            else:
                print(f"{mount_name} is already mounted")

    dbutils.fs.refreshMounts()


if __name__ == "__main__":
    main()
