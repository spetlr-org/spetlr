import unittest

from atc.utils.GetMergeStatement import GetMergeStatement


class GetMergeStatementTest(unittest.TestCase):
    def test_get_merge_statement_only_with_insert_cols(self):
        output = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            insert_cols=["col1", "col2", "col3", "col4"],
        )

        expected = (
            "MERGE INTO targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN NOT MATCHED THEN "
            "INSERT (col1, col2, col3, col4) "
            "VALUES (source.col1, source.col2, source.col3, source.col4);"
        )

        self.assertEqual(output, expected)

    def test_get_merge_statement_only_with_update_cols(self):
        output = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            update_cols=["col3", "col4"],
        )

        expected = (
            "MERGE INTO targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN MATCHED THEN UPDATE "
            "SET target.col3 = source.col3, target.col4 = source.col4;"
        )

        self.assertEqual(output, expected)

    def test_get_merge_statement_with_special_update_set(self):
        output = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            update_cols=["col3", "col4"],
            special_update_set=", target.col5 = test",
        )

        expected = (
            "MERGE INTO targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN MATCHED THEN UPDATE "
            "SET target.col3 = source.col3, target.col4 = source.col4, "
            "target.col5 = test;"
        )

        self.assertEqual(output, expected)

    def test_get_delta_merge_statement(self):
        output = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            insert_cols=["col1", "col2", "col3", "col4"],
            update_cols=["col3", "col4"],
        )

        expected = (
            "MERGE INTO targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN MATCHED THEN UPDATE "
            "SET target.col3 = source.col3, target.col4 = source.col4 "
            "WHEN NOT MATCHED THEN "
            "INSERT (col1, col2, col3, col4) "
            "VALUES (source.col1, source.col2, source.col3, source.col4);"
        )

        self.assertEqual(output, expected)

    def test_get_sql_merge_statement(self):
        output = GetMergeStatement(
            merge_statement_type="sql",
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            insert_cols=["col1", "col2", "col3", "col4"],
            update_cols=["col3", "col4"],
        )

        expected = (
            "MERGE targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN MATCHED THEN UPDATE "
            "SET target.col3 = source.col3, target.col4 = source.col4 "
            "WHEN NOT MATCHED THEN "
            "INSERT (col1, col2, col3, col4) "
            "VALUES (source.col1, source.col2, source.col3, source.col4);"
        )

        self.assertEqual(output, expected)


if __name__ == "__main__":
    unittest.main()
