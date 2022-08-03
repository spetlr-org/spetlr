import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from atc.config_master import TableConfigurator
from atc.etl.loaders.simple_sql_loader import SimpleSqlServerLoader
from atc.functions import get_unique_tempview_name
from atc.transformers.simple_sql_transformer import SimpleSqlServerTransformer
from atc.utils import DataframeCreator
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class SimpleSqlServerETLTests(unittest.TestCase):
    tc = None
    sql_server = None
    table_name = "dbo.Test1" + get_unique_tempview_name()
    table_id = "TableId1"

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()
        cls.tc = TableConfigurator()

        # Register the delivery table for the table configurator
        cls.tc.register(
            cls.table_id,
            {
                "name": cls.table_name,
            },
        )
        cls.tc.reset(debug=True)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.sql_server.drop_table(cls.table_id)
        cls.tc.reset(debug=False)

    def test01_can_transform(self):

        self.create_test_table()
        df = self.create_data()

        df_out = SimpleSqlServerTransformer(
            table_id=self.table_id, server=self.sql_server
        ).process(df)

        schema_expected = StructType(
            [
                StructField("testcolumn", IntegerType(), True),
                StructField("testcolumn2", DecimalType(12, 3), True),
                StructField("testcolumn3", StringType(), True),
            ]
        )

        self.assertEqual(df_out.schema, schema_expected)

    def test02_can_transform(self):
        df = self.create_data()

        df_out = SimpleSqlServerTransformer(
            table_id=self.table_id, server=self.sql_server
        ).process(df)

        SimpleSqlServerLoader(table_id=self.table_id, server=self.sql_server).save(
            df_out
        )

        df_with_data = self.sql_server.read_table(self.table_id)

        self.assertEqual(df_with_data.count(), 1)

    def create_test_table(self):
        sql_argument = f"""
                IF OBJECT_ID('{self.table_name}', 'U') IS NULL
                BEGIN
                CREATE TABLE {self.table_name}
                (
                testcolumn INT NULL,
                testcolumn2 DECIMAL(12,3) NULL,
                testcolumn3 [nvarchar](50) NULL,
                )
                END
                                """
        self.sql_server.execute_sql(sql_argument)

    def create_data(self) -> DataFrame:
        schema = StructType(
            [
                StructField("testcolumn", IntegerType(), True),
                StructField("testcolumn2", DoubleType(), True),
                StructField("testcolumn3", StringType(), True),
            ]
        )
        cols = ["testcolumn", "testcolumn2", "testcolumn3"]

        insert_data = (123, 1001.322, "Hello")

        df_new = DataframeCreator.make_partial(
            schema=schema, columns=cols, data=[insert_data]
        )

        return df_new.orderBy("testcolumn")
