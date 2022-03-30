import unittest

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from atc.functions import get_unique_tempview_name
from atc.spark import Spark
from atc.transformations import merge_df_into_target
from atc.utils import DataframeCreator


class MergeDfIntoTargetTest(unittest.TestCase):
    db_name = "test" + get_unique_tempview_name()
    table_name = "testTarget"

    schema = StructType(
        [
            StructField("Id", StringType(), False),
            StructField("Brand", StringType(), True),
            StructField("Model", StringType(), True),
        ]
    )

    cols = ["Id", "Brand", "Model"]

    row1 = ("1", "Fender", "Jaguar")
    row2 = ("2", "Gibson", "Starfire")
    row3 = ("3", "Ibanez", "RG")
    data_rows = [
        row1,
        row2,
        row3,
    ]
    targetrow1 = ("0", "Fender", "Telecaster")
    targetrow2 = ("2", "Gibson", "Les Paul")

    @classmethod
    def setUpClass(cls):
        cls.create_database(cls.db_name)
        cls.create_test_table(cls.table_name, cls.db_name)

    @classmethod
    def tearDownClass(cls) -> None:
        Spark.get().sql(f"drop database {cls.db_name} cascade")

    def test_01_insert(self):
        """Tests that a new row is inserted

        test table before:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   0|    Fender|      Telecaster|
        +----+----------+---------------+

        test table after:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   0|    Fender|     Telecaster|
        |   1|    Fender|          Jaguar|
        |   2|    Gibson|        Starfire|
        |   3|    Ibanez|              RG|
        +----+----------+---------------+

        """
        # Truncate table
        Spark.get().sql(f"truncate table {self.db_name}.{self.table_name}")

        # Create target data
        Spark.get().sql(
            f"INSERT INTO {self.db_name}.{self.table_name} values {self.targetrow1}"
        )

        #  Merge dataframe into target
        df = self.create_data()
        merge_df_into_target(df, self.table_name, self.db_name, ["Id"])

        # Compare
        df_expected = self.expected_data_01()
        df_result = self.get_target_table()
        self.equal_dfs(df_expected, df_result)

    def test_02_merge(self):
        """Tests that a new row is merged

         test table before:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   2|    Gibson|      Les Paul|
        +----+----------+---------------+

        test table after:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   1|    Fender|          Jaguar|
        |   2|    Gibson|        Starfire|
        |   3|    Ibanez|              RG|
        +----+----------+---------------+
        """

        # Truncate table
        Spark.get().sql(f"truncate table {self.db_name}.{self.table_name}")

        # Create target data
        Spark.get().sql(
            f"INSERT INTO {self.db_name}.{self.table_name} values {self.targetrow2}"
        )

        #  Merge dataframe into target
        df = self.create_data()
        merge_df_into_target(df, self.table_name, self.db_name, ["Id"])

        # Compare
        df_expected = self.expected_data_02()
        df_result = self.get_target_table()
        self.equal_dfs(df_expected, df_result)

    def test_03_merge_insert(self):
        """Tests that one row is merged and one inserted

         test table before:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   0|    Fender|      Telecaster|
        |   2|    Gibson|      Les Paul|
        +----+----------+---------------+

        test table after:
        +----+-----+----+----------------+
        |Id  |    Brand |           Model|
        +----+-----+----+----------------+
        |   0|    Fender|      Telecaster|
        |   1|    Fender|          Jaguar|
        |   2|    Gibson|        Starfire|
        |   3|    Ibanez|              RG|
        +----+----------+---------------+
        """

        # Truncate table
        Spark.get().sql(f"truncate table {self.db_name}.{self.table_name}")

        # Create target data
        Spark.get().sql(
            f"INSERT INTO {self.db_name}.{self.table_name} values {self.targetrow1}"
        )
        Spark.get().sql(
            f"INSERT INTO {self.db_name}.{self.table_name} values {self.targetrow2}"
        )

        #  Merge dataframe into target
        df = self.create_data()
        merge_df_into_target(df, self.table_name, self.db_name, ["Id"])

        # Compare
        df_expected = self.expected_data_03()
        df_result = self.get_target_table()
        self.equal_dfs(df_expected, df_result)

    @classmethod
    def create_test_table(self, table_name="testTarget", db_name="test"):
        location = f"/tmp/{db_name}/{table_name}"
        sql_argument = f"""CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
                      Id STRING,
                      Brand STRING,
                      Model STRING
                      )
                      USING DELTA
                      LOCATION '{location}'"""
        Spark.get().sql(sql_argument)

    @classmethod
    def create_database(self, db_name="test") -> None:
        location = f"/tmp/{db_name}/"
        sql_argument = f"CREATE DATABASE IF NOT EXISTS {db_name} LOCATION '{location}'"
        Spark.get().sql(sql_argument)

    def create_data(self) -> DataFrame:
        df_new = DataframeCreator.make_partial(
            schema=self.schema,
            columns=self.cols,
            data=self.data_rows,
        )

        return df_new.orderBy("Id")

    def expected_data_01(self) -> DataFrame:
        df_new = DataframeCreator.make_partial(
            schema=self.schema,
            columns=self.cols,
            data=[self.targetrow1, self.row1, self.row2, self.row3],
        )

        return df_new.orderBy("Id")

    def expected_data_02(self) -> DataFrame:
        df_new = DataframeCreator.make_partial(
            schema=self.schema,
            columns=self.cols,
            data=[self.row1, self.row2, self.row3],
        )

        return df_new.orderBy("Id")

    def expected_data_03(self) -> DataFrame:
        df_new = DataframeCreator.make_partial(
            schema=self.schema,
            columns=self.cols,
            data=[self.targetrow1, self.row1, self.row2, self.row3],
        )

        return df_new.orderBy("Id")

    def get_target_table(self):
        return Spark.get().read.table(f"{self.db_name}.{self.table_name}").orderBy("Id")

    def equal_dfs(self, df1, df2):
        df_expected_pd = df1.toPandas()
        df_result_pd = df2.toPandas()

        self.assertTrue(df_result_pd.equals(df_expected_pd))
