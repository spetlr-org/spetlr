# Updated CosmosDb class with dual auth (Account Key OR AAD Service Principal)

import hashlib
from typing import Optional, Union

from azure.core.exceptions import HttpResponseError
from azure.cosmos import CosmosClient, DatabaseProxy, PartitionKey
from azure.identity import ClientSecretCredential
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from spetlr.configurator.configurator import Configurator
from spetlr.cosmos.cosmos_base_server import CosmosBaseServer
from spetlr.cosmos.cosmos_handle import CosmosHandle
from spetlr.exceptions import NoSuchSchemaException, SpetlrException
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark


class SpetlrCosmosException(SpetlrException):
    pass


class CosmosDb(CosmosBaseServer):
    def __init__(
        self,
        # account key mode (default)
        database: str,
        account_key: str = None,
        # endpoint selection (either account_name or endpoint required)
        account_name: str = None,
        endpoint: str = None,
        # AAD Service Principal mode (set all 5 to enable)
        tenant_id: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        subscription_id: Optional[str] = None,
        resource_group: Optional[str] = None,
        # --- misc ---
        catalog_name: Optional[str] = "cosmosCatalog",
    ):
        """
        Either provide:
          - account_key  (key auth), OR
          - tenant_id + client_id + client_secret (AAD SPN auth)

        Also provide either account_name OR endpoint.
        """
        if not account_name and not endpoint:
            raise ValueError("account_name or endpoint must be set")

        self.endpoint = endpoint
        if not self.endpoint:
            self.endpoint = f"https://{account_name}.documents.azure.com:443/"

        self.database = database
        self.catalog_name = (
            catalog_name
            or hashlib.sha1(f"{endpoint}{self.database}".encode()).hexdigest()
        )

        if account_key and (tenant_id or client_id or client_secret):
            raise ValueError(
                "Both account_key and client credentials are set - "
                "choose only one method."
            )

        # Determine auth mode
        self._auth_mode = "key" if account_key else "aad"
        if self._auth_mode == "aad":
            if not (
                tenant_id
                and client_id
                and client_secret
                and subscription_id
                and resource_group
                and account_name
            ):
                raise ValueError(
                    "AAD auth selected but one of tenant_id, client_id, "
                    "client_secret, subscription_id resource_group, "
                    "account_name is missing."
                )
        else:
            # key mode must have account_key
            if not account_key:
                raise ValueError("account_key must be provided for key auth.")

        # Base Spark options shared by both modes
        self.config = {
            "spark.cosmos.accountEndpoint": self.endpoint,
            "spark.cosmos.database": database,
            "spark.cosmos.container": None,
        }

        # Append auth-specific Spark options
        if self._auth_mode == "key":
            self.account_key = account_key
            self.config["spark.cosmos.accountKey"] = account_key  # NOTE: correct casing
            # Python SDK (key)
            self.client = CosmosClient(self.endpoint, credential=account_key)
        else:
            # AAD (Service Principal)
            self.tenant_id = tenant_id
            self.client_id = client_id
            self.client_secret = client_secret
            self.subscription_id = subscription_id
            self.resource_group = resource_group
            self.account_name = account_name

            self.config.update(
                {
                    "spark.cosmos.auth.type": "ServicePrincipal",
                    "spark.cosmos.account.tenantId": tenant_id,
                    "spark.cosmos.auth.aad.clientId": client_id,
                    "spark.cosmos.auth.aad.clientSecret": client_secret,
                    "spark.cosmos.account.subscriptionId": subscription_id,
                    "spark.cosmos.account.resourceGroupName": resource_group,
                }
            )

            # Python SDK (AAD)
            aad_cred = ClientSecretCredential(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
            )
            self.client = CosmosClient(self.endpoint, credential=aad_cred)

        self._db_client: Optional[DatabaseProxy] = None

    def _mgmt(self) -> CosmosDBManagementClient:
        cred = ClientSecretCredential(
            self.tenant_id, self.client_id, self.client_secret
        )
        return CosmosDBManagementClient(
            credential=cred, subscription_id=self.subscription_id
        )

    @property
    def db_client(self):
        if self._db_client is not None:
            return self._db_client
        self._db_client = self.client.get_database_client(self.database)
        return self._db_client

    def _apply_spark_conf(self, spark):
        """
        Apply catalog configs according to the chosen auth mode.
        """
        spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}",
            "com.azure.cosmos.spark.CosmosCatalog",
        )
        spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.spark.cosmos.accountEndpoint",
            self.endpoint,
        )

        if self._auth_mode == "key":
            spark.conf.set(
                f"spark.sql.catalog.{self.catalog_name}.spark.cosmos.accountKey",
                self.account_key,
            )
        else:
            base = f"spark.sql.catalog.{self.catalog_name}.spark.cosmos"
            spark.conf.set(f"{base}.auth.type", "ServicePrincipal")
            spark.conf.set(f"{base}.account.tenantId", self.tenant_id)
            spark.conf.set(f"{base}.auth.aad.clientId", self.client_id)
            spark.conf.set(f"{base}.auth.aad.clientSecret", self.client_secret)

    def execute_sql(self, sql: str):
        # NOTE: Not compatible with UC-enabled clusters (unchanged behavior).
        spark = Spark.get()
        self._apply_spark_conf(spark)
        return spark.sql(sql)

    def read_table_by_name(self, table_name: str, schema: DataType = None) -> DataFrame:
        config = self.config.copy()
        config["spark.cosmos.container"] = table_name
        rd = Spark.get().read.format("cosmos.oltp").options(**config)
        if schema is not None:
            # noinspection PyTypeChecker
            rd = rd.schema(schema)
        else:
            rd = rd.option("spark.cosmos.read.inferSchema.enabled", "true")
        return rd.load()

    def read_table(self, table_id: str, schema: DataType = None) -> DataFrame:
        table_name = Configurator().table_name(table_id)
        return self.read_table_by_name(table_name, schema)

    def write_table_by_name(
        self, df_source: DataFrame, table_name: str, rows_per_partition: int = None
    ):
        if (
            rows_per_partition is not None
            and df_source.count() > rows_per_partition * 2
        ):
            partitions = int(1 + df_source.count() / rows_per_partition)
            df_source = df_source.repartition(partitions)
        config = self.config.copy()
        config["spark.cosmos.container"] = table_name
        (
            df_source.write.format("cosmos.oltp")
            .options(**config)
            .mode("append")  # overwrite is not supported in CosmosDB
            .save()
        )

    def write_table(
        self, df_source: DataFrame, table_id: str, rows_per_partition: int = None
    ):
        table_name = Configurator().table_name(table_id)
        self.write_table_by_name(df_source, table_name, rows_per_partition)

    def create_database(self) -> DatabaseProxy:
        """
        This method will create the database that is passed to the class init, if it
        does not exist. Also, whether it exists or not, it will create and return the
        cosmos database object that can be used to create containers under that
        database.
        NOTE: For AAD mode you need appropriate data-plane RBAC on the Cosmos account.
        """
        if self._auth_mode == "aad":
            mgmt = self._mgmt()
            mgmt.sql_resources.begin_create_update_sql_database(
                self.resource_group,
                self.account_name,
                self.database,
                {"resource": {"id": self.database}, "options": {}},
            ).result()
            return self.client.get_database_client(self.database)
        return self.client.create_database_if_not_exists(id=self.database)

    def create_table(
        self, table_name: str, partition_key: str, offer_throughput: int
    ) -> None:
        """
        This method will create a container(table) in the database that is passed to the
        class init. Note that, if the database does not exist, it will be created by
        this method
        """

        if self._auth_mode == "aad":
            self.create_database()
            mgmt = self._mgmt()
            mgmt.sql_resources.begin_create_update_sql_container(
                self.resource_group,
                self.account_name,
                self.database,
                table_name,
                {
                    "resource": {
                        "id": table_name,
                        "partition_key": {"paths": [partition_key], "kind": "Hash"},
                    },
                    "options": {"throughput": offer_throughput},
                },
            ).result()
            return

        database = self.create_database()

        # Configure the table
        container_properties = {
            "id": table_name,
            "partition_key": PartitionKey(path=partition_key),
        }

        # Create the table
        database.create_container_if_not_exists(
            id=container_properties["id"],
            partition_key=container_properties["partition_key"],
            offer_throughput=offer_throughput,
        )

    def delete_item(
        self, table_id: str, id: Union[int, str], pk: Union[int, str] = None
    ):
        cntr = self.db_client.get_container_client(Configurator().table_name(table_id))
        cntr.delete_item(id, partition_key=pk)

    def delete_container(self, table_id: str):
        self.delete_container_by_name(Configurator().table_name(table_id))

    def delete_container_by_name(self, table_name: str):
        if self._auth_mode == "aad":
            mgmt = self._mgmt()
            mgmt.sql_resources.begin_delete_sql_container(
                self.resource_group, self.account_name, self.database, table_name
            ).result()
            return
        self.db_client.delete_container(table_name)

    def recreate_container_by_name(self, table_name: str):
        if self._auth_mode == "aad":
            mgmt = self._mgmt()

            # Read current container definition (partition key, indexing, ttl, etc.)
            c = mgmt.sql_resources.get_sql_container(
                self.resource_group, self.account_name, self.database, table_name
            )

            # Try to preserve throughput (manual or autoscale) via ARM
            options = {}
            try:
                tp = mgmt.sql_resources.get_sql_container_throughput(
                    self.resource_group, self.account_name, self.database, table_name
                )
                if getattr(tp.resource, "throughput", None):
                    options["throughput"] = tp.resource.throughput
                elif getattr(tp.resource, "autoscale_settings", None) and getattr(
                    tp.resource.autoscale_settings, "max_throughput", None
                ):
                    options["autoscale_settings"] = {
                        "maxThroughput": tp.resource.autoscale_settings.max_throughput
                    }
            except HttpResponseError:
                pass  # leave options empty if no dedicated throughput

            # Delete + recreate via ARM
            mgmt.sql_resources.begin_delete_sql_container(
                self.resource_group, self.account_name, self.database, table_name
            ).result()

            mgmt.sql_resources.begin_create_update_sql_container(
                self.resource_group,
                self.account_name,
                self.database,
                table_name,
                {
                    "resource": {
                        "id": table_name,
                        "partition_key": c.resource.partition_key,
                        "indexing_policy": c.resource.indexing_policy,
                        "default_ttl": c.resource.default_ttl,
                    },
                    "options": options,
                },
            ).result()
            return

        # --- key mode (unchanged) ---
        for container in self.db_client.list_containers():
            if container["id"] == table_name:
                break
        else:
            raise SpetlrCosmosException(f"table not found {table_name}")

        throughput_units = (
            self.db_client.get_container_client(table_name)
            .get_throughput()
            .offer_throughput
        )

        self.db_client.delete_container(table_name)
        self.db_client.create_container(
            id=container["id"],
            partition_key=container["partitionKey"],
            offer_throughput=throughput_units,
            default_ttl=container.get("defaultTtl", None),
            indexing_policy=container.get("indexingPolicy", None),
        )

    def from_tc(self, table_id: str) -> CosmosHandle:
        tc = Configurator()
        name = tc.table_name(table_id)
        rows_per_partition = tc.get(table_id, "rows_per_partition", "")
        rows_per_partition = int(rows_per_partition) if rows_per_partition else None

        try:
            schema = SchemaManager().get_schema(table_id)
        except NoSuchSchemaException:
            schema = None

        return CosmosHandle(
            name=name,
            cosmos_db=self,
            schema=schema,
            rows_per_partition=rows_per_partition,
            partition_key=tc.get(table_id, "partition_key", None),
        )

    def delete_database(self, database: str = None):
        if not database:
            database = self.database

        if self._auth_mode == "aad":
            self._mgmt().sql_resources.begin_delete_sql_database(
                self.resource_group, self.account_name, database
            ).result()
        else:
            self.client.delete_database(database)
