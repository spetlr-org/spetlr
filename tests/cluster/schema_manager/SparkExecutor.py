from atc.sql.SqlExecutor import SqlExecutor
from tests.cluster.schema_manager import extras


class SparkSqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras)
