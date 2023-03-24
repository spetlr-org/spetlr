from decimal import Decimal

from atc_tools.testing import DataframeTestCase
from atc_tools.time import dt_utc
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from atc import Configurator
from atc.etl.loaders import SimpleLoader
from atc.functions import get_unique_tempview_name
from atc.spark import Spark
from atc.transformers.simple_sql_transformer import SimpleSqlServerTransformer
from atc.utils import DataframeCreator
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class SimpleSqlServerETLTests(DataframeTestCase):
    tc = None
    sql_server = None
    table_name = "dbo.Test1" + get_unique_tempview_name()
    table_id = "TableId1"

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()
        cls.tc = Configurator()
        cls.tc.clear_all_configurations()

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

        # Test that timestamp before has miliseconds also
        date_test = df.select("testcolumn4").collect()[0][0]
        self.assertEqual(
            dt_utc(2021, 1, 1, 14, 45, 22, 32).replace(tzinfo=None), date_test
        )

        df_out = SimpleSqlServerTransformer(
            table_id=self.table_id, server=self.sql_server
        ).process(df)

        # Check that timestamp are truncated down to only seconds
        # The 32 miliseconds should be gone.
        date_test = df_out.select("testcolumn4").collect()[0][0]
        self.assertEqual(dt_utc(2021, 1, 1, 14, 45, 22).replace(tzinfo=None), date_test)

        schema_expected = StructType(
            [
                StructField("testcolumn", IntegerType(), True),
                StructField("testcolumn2", DecimalType(12, 3), True),
                StructField("testcolumn3", StringType(), True),
                StructField("testcolumn4", TimestampType(), True),
            ]
        )

        self.assertEqual(df_out.schema, schema_expected)

    def test02_can_transform_and_load(self):
        # mix up the column spelling to test case-insensitive matching this time
        df = Spark.get().createDataFrame(
            [(123, 1001.322, "Hello", dt_utc(2021, 1, 1, 14, 45, 22, 32))],
            """
                TeStCoLuMn INT,
                TESTcolumn2 DOUBLE,
                testCOLUMN3 STRING,
                TestColumn4 TIMESTAMP
            """,
        )

        # Use transformer
        df_out = SimpleSqlServerTransformer(
            table_id=self.table_id, server=self.sql_server, ignoreCase=True
        ).process(df)

        SimpleLoader(handle=self.sql_server.from_tc(self.table_id)).save(df_out)

        df_with_data = self.sql_server.read_table(self.table_id)

        # Test that the datarow can be read
        self.assertDataframeMatches(
            df_with_data,
            None,
            [(123, Decimal("1001.322"), "Hello", dt_utc(2021, 1, 1, 14, 45, 22))],
        )

    def create_test_table(self):
        sql_argument = f"""
                IF OBJECT_ID('{self.table_name}', 'U') IS NULL
                BEGIN
                CREATE TABLE {self.table_name}
                (
                testcolumn INT NULL,
                testcolumn2 DECIMAL(12,3) NULL,
                testcolumn3 [nvarchar](50) NULL,
                testcolumn4 DATETIME NULL
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
                StructField("testcolumn4", TimestampType(), True),
            ]
        )
        cols = ["testcolumn", "testcolumn2", "testcolumn3", "testcolumn4"]

        insert_data = (123, 1001.322, "Hello", dt_utc(2021, 1, 1, 14, 45, 22, 32))

        df_new = DataframeCreator.make_partial(
            schema=schema, columns=cols, data=[insert_data]
        )

        return df_new.orderBy("testcolumn")
