from typing import Any, Dict, List, Union

import pyspark.sql.types as T
from pyspark.sql import DataFrame


class TableHandle:
    def read(self) -> DataFrame:
        raise NotImplementedError()

    def overwrite(
        self, df: DataFrame, mergeSchema: bool = None, overwriteSchema: bool = None
    ) -> None:
        raise NotImplementedError()

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        raise NotImplementedError()

    def truncate(self) -> None:
        raise NotImplementedError()

    def drop(self) -> None:
        raise NotImplementedError()

    def drop_and_delete(self) -> None:
        raise NotImplementedError()

    def write_or_append(self, df: DataFrame, mode: str) -> None:
        raise NotImplementedError()

    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        raise NotImplementedError()

    def get_tablename(self) -> str:
        raise NotImplementedError()

    def read_stream(self):
        raise NotImplementedError()
        pass

    def delete_data(
        self, comparison_col: str, comparison_limit: Any, comparison_operator: str
    ) -> None:
        pass

    def get_table_id(self):
        raise NotImplementedError()

    def get_schema(self) -> T.StructType:
        raise NotImplementedError()

    def set_schema(self, schema: T.StructType) -> "TableHandle":
        raise NotImplementedError()

    def get_options(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_options(self, options: Dict[str, Any]) -> "TableHandle":
        raise NotImplementedError()
