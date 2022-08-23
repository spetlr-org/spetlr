import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StructField, StructType

from atc.config_master import TableConfigurator
from atc.utils import DataframeCreator

from . import extras
from .DeliverySqlExecutor import DeliverySqlExecutor
from .DeliverySqlServer import DeliverySqlServer


class SqlHandleTests(unittest.TestCase):
    v1_dh = None
    t2_dh = None
    t1_dh = None
    tc = None
    sql_server = None

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()
        cls.tc = TableConfigurator()

        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

        DeliverySqlExecutor().execute_sql_file("*")

        cls.t1_dh = cls.sql_server.get_handle("SqlTestTable1")
        cls.t2_dh = cls.sql_server.get_handle("SqlTestTable2")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.t1_dh.drop()
        cls.t2_dh.drop()
        v1 = cls.tc.table_name("SqlTestView")
        cls.sql_server.drop_view_by_name(v1)

        cls.tc.reset(debug=False)

    def test_01_can_connect(self):
        self.sql_server.test_odbc_connection()
        self.assertTrue(True)

    def test04_read_w_id(self):
        # This might fail if the previous test didnt succeed
        self.t1_dh.read()
        self.t2_dh.read()
        self.assertTrue(True)

    def test05_write_w_id(self):
        df = self.create_data()
        self.t1_dh.overwrite(df)
        df_with_data = self.t1_dh.read()
        self.assertEqual(df_with_data.count(), 1)

    def test06_truncate_w_id(self):

        # Truncate
        self.t1_dh.truncate()
        df_without_data = self.t1_dh.read()
        self.assertEqual(df_without_data.count(), 0)

    def test13_drop_w_id(self):
        self.t1_dh.drop()

        table1_name = self.tc.table_name("SqlTestTable1")
        sql_argument = f"""
                (SELECT * FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = '{table1_name}') target
                """
        table_exists = self.sql_server.load_sql(sql_argument)
        self.assertEqual(table_exists.count(), 0)

    def create_data(self) -> DataFrame:
        schema = StructType(
            [
                StructField("testcolumn", IntegerType(), True),
            ]
        )
        cols = ["testcolumn"]
        df_new = DataframeCreator.make_partial(
            schema=schema, columns=cols, data=[(456,)]
        )

        return df_new.orderBy("testcolumn")
