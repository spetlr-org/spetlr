from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from atc.sql import BaseServer


class CosmosBaseServer(BaseServer):
    def execute_sql(self, sql: str):
        pass

    def read_table_by_name(self, table_name: str, schema: DataType = None) -> DataFrame:
        pass

    def read_table(self, table_id: str, schema: DataType = None) -> DataFrame:
        pass

    def write_table_by_name(
        self, df_source: DataFrame, table_name: str, rows_per_partition: int = None
    ):
        pass

    def write_table(
        self, df_source: DataFrame, table_id: str, rows_per_partition: int = None
    ):
        pass

    def delete_item(
        self, table_id: str, id: Union[int, str], pk: Union[int, str] = None
    ):
        pass

    def delete_container(self, table_id: str):
        pass

    def delete_container_by_name(self, table_name: str):
        pass

    def create_container_by_name(self, table_name: str):
        pass
