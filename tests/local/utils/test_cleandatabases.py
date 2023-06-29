import unittest

from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.testutils import CleanupTestDatabases


class CleanUpDatabasesAndTablesUnitTest(unittest.TestCase):
    def test_01_debug_databases(self):
        with self.assertRaises(OnlyUseInSpetlrDebugMode):
            CleanupTestDatabases()
