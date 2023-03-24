import random
import unittest
from unittest.mock import create_autospec

from atc_tools.testing.DataframeTestCase import DataframeTestCase
from pyspark.sql.types import IntegerType, StructField, StructType

from atc.functions import get_unique_tempview_name
from atc.spark import Spark
from atc.sql import SqlHandle
from atc.sql.sql_handle import SqlHandleInvalidName
from atc.sql.SqlBaseServer import SqlBaseServer


class SqlHandleTests(DataframeTestCase):
    sql_server_mock = None
    table_handle = None
    test_table_name = get_unique_tempview_name()
    test_join_cols = ["testcolumn"]
    test_df_data = [(random.getrandbits(16),)]
    test_df_schema = StructType(
        [
            StructField("testcolumn", IntegerType(), True),
        ]
    )
    test_df = Spark.get().createDataFrame(data=test_df_data, schema=test_df_schema)

    def setUp(self):
        self.sql_server_mock = create_autospec(SqlBaseServer)
        self.table_handle = SqlHandle(
            name=self.test_table_name, sql_server=self.sql_server_mock
        )

    def test_01_validate_with_table_name(self):
        _ = SqlHandle(
            name="tablename",
            sql_server=self.sql_server_mock,
        )
        self.assertTrue(True)

    def test_02_validate_with_db_name_and_table_name(self):
        _ = SqlHandle(
            name="dbname.tablename",
            sql_server=self.sql_server_mock,
        )
        self.assertTrue(True)

    def test_03_not_valid_name(self):
        with self.assertRaises(SqlHandleInvalidName):
            _ = SqlHandle(
                name="dbname.tablename.test",
                sql_server=self.sql_server_mock,
            )

        self.assertTrue(True)

    def test_04_read(self):
        self.sql_server_mock.read_table_by_name.return_value = self.test_df

        df_return = self.table_handle.read()
        self.assertDataframeMatches(df=df_return, expected_data=self.test_df_data)

        self.assertEqual(self.sql_server_mock.read_table_by_name.call_count, 1)
        args = self.sql_server_mock.read_table_by_name.call_args[1]
        self.assertEqual(args["table_name"], self.test_table_name)

    def test_04_overwrite(self):
        self.table_handle.overwrite(df=self.test_df)

        self.assertEqual(self.sql_server_mock.write_table_by_name.call_count, 1)
        args = self.sql_server_mock.write_table_by_name.call_args[1]

        self.assertDataframeMatches(
            df=args["df_source"], expected_data=self.test_df_data
        )
        self.assertEqual(args["table_name"], self.test_table_name)
        self.assertFalse(args["append"])

    def test_05_append(self):
        self.table_handle.append(df=self.test_df)

        self.assertEqual(self.sql_server_mock.write_table_by_name.call_count, 1)
        args = self.sql_server_mock.write_table_by_name.call_args[1]

        self.assertDataframeMatches(
            df=args["df_source"], expected_data=self.test_df_data
        )
        self.assertEqual(args["table_name"], self.test_table_name)
        self.assertTrue(args["append"])

    def test_06_write_or_append_overwrite(self):
        self.table_handle.write_or_append(df=self.test_df, mode="overwrite")

        self.assertEqual(self.sql_server_mock.write_table_by_name.call_count, 1)
        args = self.sql_server_mock.write_table_by_name.call_args[1]

        self.assertDataframeMatches(
            df=args["df_source"], expected_data=self.test_df_data
        )
        self.assertEqual(args["table_name"], self.test_table_name)
        self.assertFalse(args["append"])

    def test_07_write_or_append_append(self):
        self.table_handle.write_or_append(df=self.test_df, mode="append")

        self.assertEqual(self.sql_server_mock.write_table_by_name.call_count, 1)
        args = self.sql_server_mock.write_table_by_name.call_args[1]

        self.assertDataframeMatches(
            df=args["df_source"], expected_data=self.test_df_data
        )
        self.assertEqual(args["table_name"], self.test_table_name)
        self.assertTrue(args["append"])

    def test_07_upsert(self):
        self.table_handle.upsert(df=self.test_df, join_cols=self.test_join_cols)

        self.assertEqual(self.sql_server_mock.upsert_to_table_by_name.call_count, 1)
        args = self.sql_server_mock.upsert_to_table_by_name.call_args[1]

        self.assertDataframeMatches(
            df=args["df_source"], expected_data=self.test_df_data
        )
        self.assertEqual(args["table_name"], self.test_table_name)
        self.assertEqual(args["join_cols"], self.test_join_cols)

    def test_08_truncate(self):
        self.table_handle.truncate()

        self.assertEqual(self.sql_server_mock.truncate_table_by_name.call_count, 1)
        args = self.sql_server_mock.truncate_table_by_name.call_args[1]

        self.assertEqual(args["table_name"], self.test_table_name)

    def test_09_drop(self):
        self.table_handle.drop()

        self.assertEqual(self.sql_server_mock.drop_table_by_name.call_count, 1)
        args = self.sql_server_mock.drop_table_by_name.call_args[1]

        self.assertEqual(args["table_name"], self.test_table_name)

    def test_10_drop_and_delete(self):
        self.table_handle.drop_and_delete()

        self.assertEqual(self.sql_server_mock.drop_table_by_name.call_count, 1)
        args = self.sql_server_mock.drop_table_by_name.call_args[1]

        self.assertEqual(args["table_name"], self.test_table_name)


if __name__ == "__main__":
    unittest.main()
