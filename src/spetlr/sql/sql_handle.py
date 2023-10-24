from datetime import date, datetime
from typing import Any, List, Union

from pyspark.sql import DataFrame

from spetlr.exceptions import SpetlrException
from spetlr.sql.SqlBaseServer import SqlBaseServer
from spetlr.tables.TableHandle import TableHandle


class SqlHandleException(SpetlrException):
    pass


class SqlHandleInvalidName(SqlHandleException):
    pass


class SqlHandleInvalidFormat(SqlHandleException):
    pass


class SqlHandle(TableHandle):
    def __init__(self, name: str, sql_server: SqlBaseServer):
        self._name = name
        self._sql_server = sql_server

        self._validate()

    def _validate(self):
        """Validates that the name is either db.table or just table."""
        name_parts = self._name.split(".")
        if len(name_parts) == 1:
            self._db = None
            self._table_name = name_parts[0]
        elif len(name_parts) == 2:
            self._db = name_parts[0]
            self._table_name = name_parts[1]
        else:
            raise SqlHandleInvalidName(f"Could not parse name {self._name}")

    def read(self) -> DataFrame:
        """Read table by path if location is given, otherwise from name."""
        return self._sql_server.read_table_by_name(table_name=self._name)

    def write_or_append(self, df: DataFrame, mode: str) -> None:
        assert mode in {"append", "overwrite"}

        append = True if mode == "append" else False

        return self._sql_server.write_table_by_name(
            df_source=df, table_name=self._name, append=append
        )

    def overwrite(
        self, df: DataFrame, mergeSchema: bool = None, overwriteSchema: bool = None
    ) -> None:
        if mergeSchema is not None or overwriteSchema is not None:
            raise ValueError("Schema evolution not supported in sql server")
        return self._sql_server.write_table_by_name(
            df_source=df, table_name=self._name, append=False
        )

    def append(
        self,
        df: DataFrame,
        mergeSchema: bool = None,
    ) -> None:
        if mergeSchema is not None:
            raise ValueError("Schema evolution not supported in sql server")
        return self._sql_server.write_table_by_name(
            df_source=df, table_name=self._name, append=True
        )

    def upsert(self, df: DataFrame, join_cols: List[str]) -> Union[DataFrame, None]:
        return self._sql_server.upsert_to_table_by_name(
            df_source=df, table_name=self._name, join_cols=join_cols
        )

    def truncate(self) -> None:
        self._sql_server.truncate_table_by_name(table_name=self._name)

    def drop(self) -> None:
        self._sql_server.drop_table_by_name(table_name=self._name)

    def drop_and_delete(self) -> None:
        self.drop()

    def get_tablename(self) -> str:
        return self._name

    def delete_data(
        self, comparison_col: str, comparison_limit: Any, comparison_operator: str
    ) -> None:
        needs_quotes = (
            isinstance(comparison_limit, str)
            or isinstance(comparison_limit, date)
            or isinstance(comparison_limit, datetime)
        )
        limit = f"'{comparison_limit}'" if needs_quotes else comparison_limit
        sql_str = (
            f"DELETE FROM {self._name}"
            f" WHERE {comparison_col} {comparison_operator} {limit};"
        )
        self._sql_server.execute_sql(sql_str)
