import unittest

import atc.cosmos
from atc.config_master import TableConfigurator
from atc.functions import init_dbutils
from atc.spark import Spark


class TestCosmos(atc.cosmos.CosmosDb):
    def __init__(self):
        dbutils = init_dbutils()
        super().__init__(
            endpoint=dbutils.secrets.get("values", "Cosmos--Endpoint"),
            account_key=dbutils.secrets.get("secrets", "Cosmos--AccountKey"),
            database="AtcCosmosContainer",
        )


class CosmosTests(unittest.TestCase):
    def test_01_tables(self):
        tc = TableConfigurator()
        tc.clear_all_configurations()
        tc.register("CmsTbl", {"name": "CosmosTable"})

    def test_02_create_db(self):
        cm = TestCosmos()
        cm.execute_sql(f"CREATE DATABASE IF NOT EXISTS cosmosCatalog.{cm.database};")
        tc = TableConfigurator()
        cm.execute_sql(
            "CREATE TABLE IF NOT EXISTS"
            f" cosmosCatalog.{cm.database}.{tc.table_name('CmsTbl')}"
            " using cosmos.oltp"
            " TBLPROPERTIES(partitionKeyPath = '/pk', manualThroughput = '1100')"
        )

    def test_03_write_table(self):
        cm = TestCosmos()
        df = Spark.get().createDataFrame(
            [("first", "pk1", 56), ("second", "pk2", 987)],
            "id string, pk string, value int",
        )
        cm.write_table(df, "CmsTbl")

    def test_04_read_table(self):
        cm = TestCosmos()
        df = cm.read_table("CmsTbl")
        data = set(tuple(row) for row in df.collect())
        self.assertEqual({("first", "pk1", 56), ("second", "pk2", 987)}, data)

    def test_05_delete_items(self):
        cm = TestCosmos()
        cm.delete_item("CmsTbl", "first", "pk1")
        cm.delete_item("CmsTbl", "second", "pk2")

        cm.client.delete_database(cm.database)
