from typing import Union

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from atc.sql.CommonBaseServer import CommonBaseServer


class CosmosBaseServer(CommonBaseServer):
    # this is a typing.Protocol and will never be instantiated directly.
    def execute_sql(self, sql: str):
        pass

    def read_table_by_name(self, table_name: str, schema: DataType = None) -> DataFrame:
        """get container contents to df"""
        pass

    def read_table(self, table_id: str, schema: DataType = None) -> DataFrame:
        """get container data based on Configurator handle"""
        pass

    def write_table_by_name(
        self, df_source: DataFrame, table_name: str, rows_per_partition: int = None
    ):
        """write df to cosmos container. Only append is supported."""
        pass

    def write_table(
        self, df_source: DataFrame, table_id: str, rows_per_partition: int = None
    ):
        """write df to cosmos container based on Configurator handle.
        Only append is supported."""
        pass

    def delete_item(
        self, table_id: str, id: Union[int, str], pk: Union[int, str] = None
    ):
        """delete individual item from cosmos container
        based on Configurator handle."""
        pass

    def delete_container(self, table_id: str):
        """delete entire cosmos container based on Configurator handle."""
        pass

    def delete_container_by_name(self, table_name: str):
        """delete entire cosmos container by name."""
        pass

    def recreate_container_by_name(self, table_name: str):
        """extract container details from existing,
        delete the container, then create it again with the same details.
        based on Configurator handle."""
        pass
