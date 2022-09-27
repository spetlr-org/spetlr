from typing import List

from pyspark.sql import DataFrame

from atc.sql.CommonBaseServer import CommonBaseServer


class SqlBaseServer(CommonBaseServer):
    def load_sql(self, sql: str) -> DataFrame:
        pass

    def upsert_to_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        join_cols: List[str],
    ):
        pass

    def truncate_table_by_name(self, table_name: str) -> None:
        pass

    def drop_table_by_name(self, table_name: str) -> None:
        pass
