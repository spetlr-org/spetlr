import warnings

from spetlr.sql import SqlServer
from spetlr.sql.SqlExecutor import SqlExecutor
from spetlr.utils.sqlcleantables import sqlcleanall


class SqlCleanupTestTables(SqlExecutor):
    """
    This class can be used for removing all SPETLR test
    tables from a SqlServer database.

    The tables must have been created more than 30 minutes ago

    Todo: This class is untested. How could this be done?
    """

    def __init__(self, server: SqlServer):
        super().__init__(base_module=sqlcleanall, server=server)

    # Convenience method since the class always executes clean-all-tests (*)

    def execute(self):
        warnings.warn("SqlCleanupTestTables() is an untested class.")
        self.execute_sql_file("*")
