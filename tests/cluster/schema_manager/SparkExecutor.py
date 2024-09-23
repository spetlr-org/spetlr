from spetlr.sql.SqlExecutor import SqlExecutor

from . import extras


class SparkSqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras)
