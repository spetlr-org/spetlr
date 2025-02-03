import unittest
from textwrap import dedent

from spetlr import Configurator
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

        sqlExe = SqlExecutor(ignore_location=True)

        result = sqlExe._handle_line_regex(statement_with_location)

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

        sqlExe = SqlExecutor(ignore_lines_regex=["DROP", "USE"])

        result = sqlExe._handle_line_regex(statement_without_match)

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

        sqlExe = SqlExecutor(ignore_location=True)

        result = sqlExe._handle_line_regex(statement_with_partial_match)

        self.assertEqual(result, expected_statement)

    def test_11_file_by_name_remove_location(self):
        sqlexe = SqlExecutor(sql, statement_spliter=None, ignore_location=True)

        statements = sqlexe.get_statements("test1")
        _all = [x.strip() for x in statements]  # Stripping for clean comparison

        for a in _all:
            if "LOCATION" in a:
                raise Exception("LOCATION found!")

    def test_12_file_by_name_semicolon_after_location(self):
        sqlexe = SqlExecutor(sql, ignore_location=True)

        statements = list(sqlexe.get_statements("test_location"))

        self.assertEqual(len(statements), 2)

        expected1 = """
        CREATE TABLE IF NOT EXISTS some.table
        (hi int)"""

        self.assertEqual(dedent(statements[0]).strip(), dedent(expected1).strip())

        expected2 = """
        CREATE TABLE IF NOT EXISTS some.other
        (there int)
        ;"""

        self.assertEqual(dedent(statements[1]).strip(), dedent(expected2).strip())

    def test_13_no_changes_if_no_ignore(self):
        """
        This test ensures that all are preserved correctly
        even if no ignores are defined
        """

        sqlexe = SqlExecutor(sql)
        statements = list(sqlexe.get_statements("test_location"))

        self.assertEqual(len(statements), 2)

        expected1 = """
        CREATE TABLE IF NOT EXISTS some.table
        (hi int)
        LOCATION somepath/;"""

        expected2 = """
        \n\n
        CREATE TABLE IF NOT EXISTS some.other
        (there int)
        ;
        """

        self.assertEqual(dedent(statements[0]).strip(), dedent(expected1).strip())

        self.assertEqual(dedent(statements[1]).strip(), dedent(expected2).strip())

    def test_14_ignore_location_lines(self):
        self._setup_clear_conf()
        sqlexe = SqlExecutor(sql, ignore_location=True)

        sql_statement = """
        CREATE TABLE IF NOT EXISTS my_test_db.table1
        (
          id INT
        )
        LOCATION "/mnt/foo/bar/my_test_db/table1/";
        """
        cleaned_sql = list(sqlexe.chop_and_substitute(sql_statement))
        self.assertNotIn("LOCATION", cleaned_sql[0])

    def test_replace_curly_bracket_placeholders(self):
        """Test replacement of {MNT} in SQL using Configurator."""
        self._setup_clear_conf()
        sqlexe = SqlExecutor(sql)
        sql_statement = """
           CREATE TABLE IF NOT EXISTS my_test_db.test
           (
             id INT
           )
           LOCATION "/{MNT}/foo/bar/my_test_db/test/";
           """
        cleaned_sql = list(sqlexe.chop_and_substitute(sql_statement))
        self.assertIn("/mnt/test/path/foo/bar/my_test_db/test/", cleaned_sql[0])

    def test_ignore_multiple_patterns(self):
        """Test multiple regex patterns for filtering lines."""

        self._setup_clear_conf()
        sqlexe = SqlExecutor(
            sql, ignore_lines_regex=[r"^LOCATION\b.*", r"^-- spetlr.*"]
        )

        sql_statement = """
        -- spetlr.Configurator key: TestTable
        CREATE TABLE IF NOT EXISTS my_test_db.test
        (
          id INT
        )
        LOCATION "/mnt/foo/bar/my_test_db/test/";
        """
        cleaned_sql = list(sqlexe.chop_and_substitute(sql_statement))
        self.assertNotIn("LOCATION", cleaned_sql[0])
        self.assertNotIn("spetlr.Configurator", cleaned_sql[0])

    def test_ignore_table_drops(self):
        """Test filtering out DROP TABLE statements."""

        self._setup_clear_conf()
        sqlexe = SqlExecutor(sql, ignore_lines_regex=[r"^DROP TABLE.*"])

        sql_statement = """
        DROP TABLE my_test_db.old_table;
        CREATE TABLE IF NOT EXISTS my_test_db.new_table
        (
          name STRING
        );
        """
        cleaned_sql = list(sqlexe.chop_and_substitute(sql_statement))
        self.assertNotIn("DROP TABLE", cleaned_sql[0])
        self.assertIn("CREATE TABLE", cleaned_sql[0])

    def _setup_clear_conf(self):
        Configurator().clear_all_configurations()
        Configurator().register("MNT", "/mnt/test/path")
