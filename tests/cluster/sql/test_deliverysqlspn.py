from atc import Configurator

from . import extras
from .DeliverySqlServerSpn import DeliverySqlServerSpn
from .test_deliverysql import DeliverySqlServerTests


class DeliverySqlServerSpnTests(DeliverySqlServerTests):
    @classmethod
    def setUpClass(cls):
        cls.sql_server = DeliverySqlServerSpn()
        cls.tc = Configurator()

        cls.tc.add_resource_path(extras)
        cls.tc.reset(debug=True)
