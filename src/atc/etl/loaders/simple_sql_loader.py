from pyspark.sql import DataFrame

from atc.etl import Loader
from atc.sql.SqlServer import SqlServer


class SimpleSqlServerLoader(Loader):
    def __init__(
        self,
        *,
        table_id: str,
        server: SqlServer,
        append: bool = False,
    ):
        super().__init__()
        self.server = server
        self.table_id = table_id
        self.append = append

    def save(self, df: DataFrame) -> None:

        self.server.write_table(df, self.table_id, append=self.append)
