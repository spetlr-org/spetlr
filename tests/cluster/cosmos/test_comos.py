import unittest

import atc.cosmos
from atc.config_master import TableConfigurator
from atc.functions import init_dbutils


class TestCosmos(atc.cosmos.CosmosDb):
    def __init__(self):
        super().__init__(
            account_name="atc",
            account_key=init_dbutils().secrets.get("atc", "Cosmos--AccountKey"),
            database="AtcCosmosContainer",
        )


class ApiTests(unittest.TestCase):
    def test_01_tables(self):
        tc = TableConfigurator()
        tc.clear_all_configurations()
        tc.register("CmsTbl", {"name": "CosmosTable"})

    def test_02_create_db(self):
        cm = TestCosmos()
        cm.execute_sql(
            "CREATE DATABASE IF NOT EXISTS cosmosCatalog.AtcCosmosContainer;"
        )
        cm.execute_sql(
            "CREATE TABLE IF NOT EXISTS cosmosCatalog.AtcCosmosContainer.CosmosTable"
            " using cosmos.oltp"
            " TBLPROPERTIES(partitionKeyPath = '/id', manualThroughput = '1100')"
        )
