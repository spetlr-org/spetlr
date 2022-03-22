import unittest
from atc.functions import get_unique_tempview_name, drop_table_cascade, init_dbutils
from atc.spark import Spark
from atc.atc_exceptions import NoTableException


class DropTableCascadeTest(unittest.TestCase):
    db_name = "test" + get_unique_tempview_name()
    table_name = "testTarget"

    @classmethod
    def setUpClass(cls):
        cls.create_database(cls.db_name)
        cls.create_test_table(cls.table_name, cls.db_name)

    @classmethod
    def tearDownClass(cls) -> None:
        Spark.get().sql(f"drop database if exists {cls.db_name} cascade")

    def test_01_drop_table_cascade(self):
        # TODO: Implement if path exists. It should NOT!
        # Get table path
        # table_path = str(
        #     Spark.get()
        #     .sql(f"DESCRIBE DETAIL {self.db_name}.{self.table_name}")
        #     .collect()[0]["location"]
        # )

        # Drop table casecde
        drop_table_cascade(f"{self.db_name}.{self.table_name}")

        # TODO: Implement if path exists. It should NOT!
        # Should not be able to find table in directory
        # self.assertRaises(Exception, init_dbutils().fs.ls(table_path))

    def test_02_drop_table_exception(self):
        with self.assertRaises(NoTableException) as cm:
            drop_table_cascade(f"{self.db_name}.NoExistingTable")
        the_exception = cm.exception

        self.assertEqual(the_exception.value, "No table found!")

    @classmethod
    def create_test_table(self, table_name="testTarget", db_name="test"):
        location = f"/tmp/{db_name}/{table_name}"
        sql_argument = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
                        Id STRING,
                        sometext STRING,
                        someinteger INT
                        )
                        USING DELTA
                        LOCATION {location}"""
        Spark.get().sql(sql_argument)

    @classmethod
    def create_database(self, db_name="test") -> None:
        location = f"/tmp/{db_name}/"
        sql_argument = f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION {location}"
        Spark.get().sql(sql_argument)
