from typing import Any, List, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spetlr.cosmos.cosmos_base_server import CosmosBaseServer
from spetlr.exceptions import SpetlrException
from spetlr.tables.TableHandle import TableHandle


class CosmosHandleException(SpetlrException):
    pass


class CosmosHandleInvalidName(CosmosHandleException):
    pass


class CosmosHandleInvalidFormat(CosmosHandleException):
    pass


class CosmosHandle(TableHandle):
    def __init__(
        self,
        name: str,
        cosmos_db: CosmosBaseServer,
        rows_per_partition: int = None,
        schema: StructType = None,
    ):
        self._name = name
        self._cosmos_db = cosmos_db
        self._rows_per_partition = rows_per_partition
        self._schema = schema

    def read(self) -> DataFrame:
        return self._cosmos_db.read_table_by_name(
            table_name=self._name, schema=self._schema
        )

    def recreate(self):
        self._cosmos_db.recreate_container_by_name(self._name)

    def append(
        self,
        df: DataFrame,
        mergeSchema: bool = None,
    ) -> None:
        # ignore mergeSchema silently. Schema evolution works implicitly in cosmos
        self._cosmos_db.write_table_by_name(df, self._name, self._rows_per_partition)

    def truncate(self) -> None:
        self.recreate()

    def drop(self) -> None:
        self._cosmos_db.delete_container_by_name(self._name)

    def drop_and_delete(self) -> None:
        self.drop()

    def write_or_append(self, df: DataFrame, mode: str) -> None:
        if mode == "append":
            return self.append(df)
        else:
            raise ValueError(f"unsupported flag value of mode: {mode}")

    def overwrite(
        self, df: DataFrame, mergeSchema: bool = None, overwriteSchema: bool = None
    ) -> None:
        raise NotImplementedError("Method not supported in Cosmos")

    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        raise NotImplementedError("Method not supported in Cosmos")

    def get_tablename(self) -> str:
        return self._name

    def delete_data(
        self, comparison_col: str, comparison_limit: Any, comparison_operator: str
    ) -> None:
        raise NotImplementedError("Method not yet implemented in Cosmos")
