from typing import Protocol

from pyspark.sql import DataFrame

from atc.tables import TableHandle


class CommonBaseServer(Protocol):
    def execute_sql(self, sql: str) -> None:
        """synonym of .sql()"""
        pass

    def from_tc(self, id: str) -> TableHandle:
        pass

    def sql(self, sql: str) -> None:
        """synonym of .execute_sql()"""
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
