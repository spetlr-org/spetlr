import unittest
import uuid

from spetlr import Configurator
from spetlr.cosmos import CosmosDb
from spetlr.functions import init_dbutils
from spetlr.spark import Spark


class CosmosTests(unittest.TestCase):
    _cm: CosmosDb
    _tc: Configurator

    @classmethod
    def setUpClass(cls):
        dbutils = init_dbutils()

        cls._cm = CosmosDb(
            endpoint=dbutils.secrets.get("secrets", "Cosmos--Endpoint"),
            account_key=dbutils.secrets.get("secrets", "Cosmos--AccountKey"),
            database=f"SpetlrCosmosContainer{uuid.uuid4().hex}",
            catalog_name=None,
        )

        cls._tc = Configurator()
        cls._tc.clear_all_configurations()
        cls._tc.set_debug()
        cls._tc.register(
            "CmsTbl",
            {
                "name": "CosmosTable{ID}",
                "schema": {"sql": "id string, pk string, value int"},
                "rows_per_partition": 5,
            },
        )

    def test_01_create_db(self):
        self._cm.create_table(
            table_name=self._tc.table_name("CmsTbl"),
            partition_key="/pk",
            offer_throughput=400,
        )

    def test_02_write_table(self):
        df = Spark.get().createDataFrame(
            [("first", "pk1", 56), ("second", "pk2", 987)],
            "id string, pk string, value int",
        )
        self._cm.write_table(df, "CmsTbl")

    def test_03_read_table(self):
        df = self._cm.read_table("CmsTbl")
        data = set(tuple(row) for row in df.collect())
        self.assertEqual({("first", "pk1", 56), ("second", "pk2", 987)}, data)

    def test_04_use_handle(self):
        ch = self._cm.from_tc("CmsTbl")
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

    @classmethod
    def tearDownClass(cls):
        cls._cm.delete_item("CmsTbl", "third", "pk1")
        cls._cm.delete_item("CmsTbl", "fourth", "pk2")

        cls._cm.client.delete_database(cls._cm.database)
