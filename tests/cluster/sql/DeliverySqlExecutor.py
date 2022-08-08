from atc.sql.SqlExecutor import SqlExecutor
from tests.cluster.sql import extras
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class DeliverySqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras, server=DeliverySqlServer())
