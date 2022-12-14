import unittest

from azure.cosmos.exceptions import CosmosHttpResponseError

import atc.cosmos
from atc import Configurator
from atc.functions import init_dbutils
from atc.spark import Spark


class TestCosmos(atc.cosmos.CosmosDb):
    def __init__(self):
        dbutils = init_dbutils()
        super().__init__(
            endpoint=dbutils.secrets.get("values", "Cosmos--Endpoint"),
            account_key=dbutils.secrets.get("secrets", "Cosmos--AccountKey"),
            database="AtcCosmosContainer",
            catalog_name=None,
        )


class CosmosTests(unittest.TestCase):
    def test_01_tables(self):
        tc = Configurator()
        tc.clear_all_configurations()
        tc.register(
            "CmsTbl",
            {
                "name": "CosmosTable",
                "schema": {"sql": "id string, pk string, value int"},
                "rows_per_partition": 5,
            },
        )

    def test_02_create_db(self):
        cm = TestCosmos()
        # clean up the database in case it exists from last run
        try:
            cm.client.delete_database(cm.database)
        except CosmosHttpResponseError:
            pass

        cm.execute_sql(
            f"CREATE DATABASE IF NOT EXISTS {cm.catalog_name}.{cm.database};"
        )
        tc = Configurator()
        cm.execute_sql(
            "CREATE TABLE IF NOT EXISTS"
            f" {cm.catalog_name}.{cm.database}.{tc.table_name('CmsTbl')}"
            " using cosmos.oltp"
            " TBLPROPERTIES(partitionKeyPath = '/pk', manualThroughput = '400')"
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

    def test_05_use_handle(self):
        ch = TestCosmos().from_tc("CmsTbl")
        df = ch.read()
        data = set(tuple(row) for row in df.collect())
        self.assertEqual({("first", "pk1", 56), ("second", "pk2", 987)}, data)

        # create a df with two new rows.
        new_df = Spark.get().createDataFrame(
            [("third", "pk1", 56), ("fourth", "pk2", 987)],
            "id string, pk string, value int",
        )

        # in append mode, there will now be 4 rows.
        ch.append(new_df)
        self.assertEqual(ch.read().count(), 4)

        # drop and recreate the container
        # we should end up with only the new rows.
        ch.recreate()
        ch.append(new_df)
        self.assertEqual(ch.read().count(), 2)

    def test_10_delete_items(self):
        cm = TestCosmos()
        cm.delete_item("CmsTbl", "third", "pk1")
        cm.delete_item("CmsTbl", "fourth", "pk2")

        cm.client.delete_database(cm.database)
