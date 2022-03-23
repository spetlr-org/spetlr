import unittest

from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class DeliverySqlServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()

    def test01_can_conect(self):
        self.sql_server.test_odbc_connection()
        self.assertTrue(True)
