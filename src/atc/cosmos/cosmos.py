# Defines a class for opening a connection to Cosmos DB,
# loading and saving tables, and executing SQL.
import hashlib

# e.g. usage:
#   server = CosmosDb()
#   table_name = CosmosDb.table_name("TableId")
#   server.execute_sql(
#       f"CREATE TABLE cosmosCatalog.database_name.table_name using cosmos.oltp"
#       )
#   df = server.load_table("TableId")
#   server.save_table(df, "TableId")
from typing import Optional, Union

from azure.cosmos import CosmosClient, DatabaseProxy
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from atc.configurator.configurator import Configurator
from atc.cosmos.cosmos_base_server import CosmosBaseServer
from atc.cosmos.cosmos_handle import CosmosHandle
from atc.exceptions import AtcException, NoSuchSchemaException
from atc.schema_manager import SchemaManager
from atc.spark import Spark


class AtcCosmosException(AtcException):
    pass


class CosmosDb(CosmosBaseServer):
    def __init__(
        self,
        account_key: str,
        database: str,
        account_name: str = None,
        endpoint: str = None,
        catalog_name: Optional[str] = "cosmosCatalog",
    ):
        if not account_name and not endpoint:
            raise ValueError("account_name or endpoint must be set")

        if not endpoint:
            endpoint_pattern = "https://{}.documents.azure.com:443/"
            endpoint = endpoint_pattern.format(account_name)

        self.endpoint = endpoint
        self.account_key = account_key
        self.database = database
        self.config = {
            "spark.cosmos.accountEndpoint": self.endpoint,
            "spark.cosmos.accountkey": account_key,
            "spark.cosmos.database": database,
            "spark.cosmos.container": None,
        }
        self.client = CosmosClient(endpoint, credential=account_key)

        self._db_client: Optional[DatabaseProxy] = None
        self.catalog_name = (
            catalog_name
            or hashlib.sha1(f"{endpoint}{self.database}".encode()).hexdigest()
        )

    @property
    def db_client(self):
        if self._db_client is not None:
            return self._db_client

        self._db_client = self.client.get_database_client(self.database)
        return self._db_client

    def execute_sql(self, sql: str):
        # Examples:
        #
        # sql = f"CREATE DATABASE IF NOT EXISTS cosmosCatalog.{database_name};"
        #
        # sql = f"CREATE TABLE IF NOT EXISTS cosmosCatalog.{database_name}.{table_name}"
        #       " using cosmos.oltp "
        #       "TBLPROPERTIES(partitionKeyPath = '/id', manualThroughput = '1100')"
        spark = Spark.get()
        spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}",
            "com.azure.cosmos.spark.CosmosCatalog",
        )
        spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.spark.cosmos.accountEndpoint",
            self.endpoint,
        )
        spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}.spark.cosmos.accountKey",
            self.account_key,
        )
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

    def delete_item(
        self, table_id: str, id: Union[int, str], pk: Union[int, str] = None
    ):
        cntr = self.db_client.get_container_client(Configurator().table_name(table_id))
        cntr.delete_item(id, partition_key=pk)

    def delete_container(self, table_id: str):
        self.delete_container_by_name(Configurator().table_name(table_id))

    def delete_container_by_name(self, table_name: str):
        self.db_client.delete_container(table_name)

    def recreate_container_by_name(self, table_name: str):
        """
        Delete and recreate the container while preserving properties as
        far as possible.
        """

        for container in self.db_client.list_containers():
            if container["id"] == table_name:
                break
        else:
            raise AtcCosmosException(f"table not found {table_name}")

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
        rows_per_partition = tc.table_property(table_id, "rows_per_partition", "")
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
        )
