import unittest

from atc.utils.GetMergeStatement import GetMergeStatement


class GetMergeStatementTest(unittest.TestCase):
    def test(self):
        output = GetMergeStatement(
            target_table_name="targetname",
            source_table_name="sourcename",
            join_cols=["col1", "col2"],
            insert_cols=["col1", "col2", "col3", "col4"],
            update_cols=["col3", "col4"],
            special_update_set="",
        )

        expected = (
            "MERGE INTO targetname AS target USING sourcename AS source "
            "ON (source.col1 = target.col1) AND (source.col2 = target.col2) "
            "WHEN MATCHED THEN UPDATE -- update existing records "
            "SET target.col3 = source.col3, target.col4 = source.col4   "
            "WHEN NOT MATCHED THEN INSERT -- insert new records "
            "( col1, col2, col3, col4 ) "
            "VALUES ( source.col1, source.col2, source.col3, source.col4 );"
        )
        self.assertEqual(output.replace("  ", "").replace("\n", " ").strip(), expected)
