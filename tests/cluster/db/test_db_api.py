import unittest

from spetlr.db_auto import getDbApi


class ApiTests(unittest.TestCase):
    def test_01_configureApi(self):
        getDbApi()

    def test_02_make_a_call(self):
        db = getDbApi()

        self.assertTrue(db.dbfs.exists("/files/"))
