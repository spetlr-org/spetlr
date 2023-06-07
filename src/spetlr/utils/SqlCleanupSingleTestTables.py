from spetlr.config_master import Configurator
from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.sql import SqlServer
from spetlr.sql.SqlExecutor import SqlExecutor
from spetlr.utils.sqlcleantables import sqlcleansingle


class SqlCleanupSingleTestTables(SqlExecutor):
    """
    This class can be used for removing SOME SPETLR test
    tables from a SqlServer database.

    SOME: The tests tables that are removed,
    are those who are registered with the current
    UUID from the SPETLR Configurator.

    """

    def __init__(self, server: SqlServer):
        super().__init__(base_module=sqlcleansingle, server=server)

    # Convenience method since the class always executes clean-single-test.sql (*)
    def execute(self):
        c = Configurator()
        if not c.is_debug():
            raise OnlyUseInSpetlrDebugMode()

        self.execute_sql_file("clean-single-test")
