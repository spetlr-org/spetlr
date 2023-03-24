import unittest

from atc.sql import SqlExecutor
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
