import unittest

from tests.cluster.sql.DeliverySqlExecutor import DeliverySqlExecutor
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class DeliverySqlExecutorTests(unittest.TestCase):
    sql_server = None

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()

    @classmethod
    def tearDownClass(cls):
        cls.sql_server.drop_table("SqlTestTable1")

    def test_can_execute(self):
        DeliverySqlExecutor().execute_sql_file("test1")

        self.sql_server.read_table("SqlTestTable1")

        self.sql_server.read_table("SqlTestTable2")

        self.assertTrue(True)
