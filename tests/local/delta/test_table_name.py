# These tests will be activated when the corresponding cluster tests get fixed

# from unittest import TestCase

# from spetlr.deltaspec.exceptions import InvalidTableName
# from spetlr.deltaspec.helpers import TableName


# class TestTableName(TestCase):
#     def test_01_levels(self):
#         n = TableName.from_str(None)
#         self.assertEqual(n.level(), 0)
#         self.assertEqual(str(n), "")
#         self.assertEqual(TableName.from_str(None), TableName())
#         self.assertEqual(TableName.from_str(""), TableName())

#         t = TableName.from_str("tbl")
#         self.assertEqual(t.level(), 1)
#         self.assertEqual(str(t), "tbl")

#         d = TableName.from_str("db.tbl")
#         self.assertEqual(d.level(), 2)
#         self.assertEqual(str(d), "db.tbl")

#         c = TableName.from_str("ctlg.db.tbl")
#         self.assertEqual(c.level(), 3)
#         self.assertEqual(str(c), "ctlg.db.tbl")

#         with self.assertRaises(InvalidTableName):
#             TableName(schema="db")

#         with self.assertRaises(InvalidTableName):
#             TableName(catalog="c", schema="db")

#         self.assertEqual(str(c.to_level(2)), "db.tbl")
#         self.assertEqual(str(c.to_level(1)), "tbl")
#         self.assertEqual(str(c.to_level(0)), "")
