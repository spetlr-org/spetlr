from typing import List, Protocol

from pyspark.sql import DataFrame

from atc.tables import TableHandle


class BaseServer(Protocol):
    def execute_sql(self, sql: str) -> None:
        pass

    def from_tc(self, id: str) -> TableHandle:
        pass

    def sql(self, sql: str) -> None:
        pass

    def load_sql(self, sql: str) -> DataFrame:
        pass

    def read_table_by_name(self, table_name: str) -> DataFrame:
        pass

    def write_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        append: bool = False,
    ):
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
