import unittest

from spetlr.sql import SqlExecutor
from tests.local.sql import sql


class TestSqlExecutor(unittest.TestCase):
    def test_01_standard_splitting(self):
        s = SqlExecutor(sql)
        statements = list(s.get_statements("*"))
        self.assertEqual(len(statements), 3)

    def test_02_marker_splitting(self):
        s = SqlExecutor(sql, statement_spliter=["-- COMMAND ----------"])
        statements = list(s.get_statements("*"))
        self.assertEqual(len(statements), 2)

    def test_03_no_splitting(self):
        s = SqlExecutor(sql, statement_spliter=None)
        statements = list(s.get_statements("*"))
        self.assertEqual(len(statements), 1)

    def test_04_file_by_name(self):
        s = SqlExecutor(sql, statement_spliter=None)
        statements = list(s.get_statements("test1"))
        self.assertEqual(len(statements), 1)

    def test_05_file_not_found(self):
        with self.assertRaises(ValueError):
            s = SqlExecutor(sql, statement_spliter=None)
            list(s.get_statements("unknown"))

    def test_06_file_not_found_and_ignore_empty_folder(self):
        s = SqlExecutor(sql, statement_spliter=None, ignore_empty_folder=True)
        statements = list(s.get_statements("unknown"))
        self.assertEqual(len(statements), 0)
