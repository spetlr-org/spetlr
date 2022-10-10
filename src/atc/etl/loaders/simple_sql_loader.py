from deprecated import deprecated

from atc.sql.SqlServer import SqlServer

from . import SimpleLoader


@deprecated(reason="Use a SimpleLoader with a SqlHandle.")
class SimpleSqlServerLoader(SimpleLoader):
    def __init__(
        self,
        *,
        table_id: str,
        server: SqlServer,
        append: bool = False,
    ):
        super().__init__(
            handle=server.from_tc(table_id), mode="append" if append else "overwrite"
        )
