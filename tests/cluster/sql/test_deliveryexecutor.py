import unittest

from atc.config_master import TableConfigurator
from tests.cluster.sql.DeliverySqlExecutor import DeliverySqlExecutor
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer

from . import extras


class DeliverySqlExecutorTests(unittest.TestCase):
    tc = None
    sql_server = None

    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServer()

        # Register the delivery table for the table configurator
        cls.tc = TableConfigurator()
        cls.tc.add_resource_path(extras)
        cls.tc.set_debug()

    @classmethod
    def tearDownClass(cls):
        cls.sql_server.drop_table("SqlTestTable1")

    def test_can_execute(self):
        DeliverySqlExecutor().execute_sql_file("test1")

        self.sql_server.read_table("SqlTestTable1")

        self.sql_server.read_table("SqlTestTable2")

        self.assertTrue(True)
