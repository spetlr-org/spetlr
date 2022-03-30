import unittest

from atc.db_auto import getDbApi


class ApiTests(unittest.TestCase):
    def test_01_configureApi(self):
        getDbApi()

    def test_02_make_a_call(self):
        db = getDbApi()
        self.assertIn("files", db.dbfs.list("/"))
