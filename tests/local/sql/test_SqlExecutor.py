import unittest

from spetlr.sql import SqlExecutor
from tests.local.sql import sql


class TestSqlExecutor(unittest.TestCase):
    def test_01_standard_splitting(self):
        s = SqlExecutor(sql)
        statements = list(s.get_statements("test1"))
        self.assertEqual(len(statements), 3)

    def test_02_marker_splitting(self):
        s = SqlExecutor(sql, statement_spliter=["-- COMMAND ----------"])
        statements = list(s.get_statements("test1"))
        self.assertEqual(len(statements), 2)

    def test_03_no_splitting(self):
        s = SqlExecutor(sql, statement_spliter=None)
        statements = list(s.get_statements("test1"))
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

    def test_06_remove_location(self):
        statement_with_location = """
        create table if not exists heyson
        (col int)
        LOCATION {SOME_PATH}
        """
        expected_statement = """
        create table if not exists heyson
        (col int)
        """

        sqlExe = SqlExecutor(ignore_lines_starts_with=["LOCATION"])

        result = sqlExe._handle_line_starts_with(statement_with_location)

        self.assertEqual(result, expected_statement)

    def test_07_no_matching_lines(self):
        statement_without_match = """
        create table if not exists my_table
        (id int, name string)
        """

        expected_statement = """
        create table if not exists my_table
        (id int, name string)
        """

        sqlExe = SqlExecutor(ignore_lines_starts_with=["DROP", "USE"])

        result = sqlExe._handle_line_starts_with(statement_without_match)

        self.assertEqual(result, expected_statement)

    def test_09_partial_match_not_removed(self):
        statement_with_partial_match = """
        create table if not exists my_table
        (id int, name string);
        LOCATIONS should be specified here
        """

        expected_statement = """
        create table if not exists my_table
        (id int, name string);
        LOCATIONS should be specified here
        """

        sqlExe = SqlExecutor(ignore_lines_starts_with=["LOCATION"])

        result = sqlExe._handle_line_starts_with(statement_with_partial_match)

        self.assertEqual(result, expected_statement)

    def test_11_file_by_name_remove_location(self):
        sqlexe = SqlExecutor(
            sql, statement_spliter=None, ignore_lines_starts_with=["LOCATION"]
        )

        statements = sqlexe.get_statements("test1")
        _all = [x.strip() for x in statements]  # Stripping for clean comparison
        print(_all)
        for a in _all:
            if "LOCATION" in a:
                raise Exception("LOCATION found!")

    def test_12_file_by_name_semicolon_after_location(self):
        sqlexe = SqlExecutor(
            sql, statement_spliter=None, ignore_lines_starts_with=["LOCATION"]
        )

        statements = sqlexe.get_statements("test_location")
        _all = [x.strip() for x in statements]  # Stripping for clean comparison

        # Expects that semicolons are still there
        expected = [
            "CREATE TABLE IF NOT EXISTS some.table\n"
            "(hi int)\n"
            ";\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS some.other\n"
            "(there int)"
        ]

        self.assertEqual(_all, expected)

    def test_13_no_changes_if_no_ignore(self):
        """
        This test ensures that all are preserved correctly
        even if no ignores are defined
        """

        sqlexe = SqlExecutor(sql)
        statements = list(sqlexe.get_statements("test_location"))

        expected1 = """
        CREATE TABLE IF NOT EXISTS some.table
        (hi int)
        LOCATION somepath/;"""

        expected2 = """

        CREATE TABLE IF NOT EXISTS some.other
        (there int)
        ;
        """

        self.assertEqual(statements[0], expected1)

        self.assertEqual(statements[1], expected2)
