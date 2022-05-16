# Defines a class for opening a connection to Cosmos DB,
# loading and saving tables, and executing SQL.

# e.g. usage:
#   server = CosmosDb()
#   table_name = CosmosDb.table_name("TableId")
#   server.execute_sql(
#       f"CREATE TABLE cosmosCatalog.database_name.table_name using cosmos.oltp"
#       )
#   df = server.load_table("TableId")
#   server.save_table(df, "TableId")
from typing import Union

from azure.cosmos import CosmosClient
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from atc.config_master import TableConfigurator
from atc.spark import Spark


class CosmosDb:
    def __init__(
        self,
        account_key: str,
        database: str,
        account_name: str = None,
        endpoint: str = None,
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
            "spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog"
        )
        spark.conf.set(
            "spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint",
            self.endpoint,
        )
        spark.conf.set(
            "spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", self.account_key
        )
        spark.sql(sql)

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
        table_name = TableConfigurator().table_name(table_id)
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
        table_name = TableConfigurator().table_name(table_id)
        self.write_table_by_name(df_source, table_name, rows_per_partition)

    def delete_item(
        self, table_id: str, id: Union[int, str], pk: Union[int, str] = None
    ):
        db = self.client.get_database_client(self.database)
        cntr = db.get_container_client(TableConfigurator().table_name(table_id))
        cntr.delete_item(id, partition_key=pk)
