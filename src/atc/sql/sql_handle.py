from typing import List, Union

from pyspark.sql import DataFrame

from atc.exceptions import AtcException
from atc.sql.SqlBaseServer import SqlBaseServer
from atc.tables.TableHandle import TableHandle


class SqlHandleException(AtcException):
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

    def overwrite(self, df: DataFrame) -> None:
        return self._sql_server.write_table_by_name(
            df_source=df, table_name=self._name, append=False
        )

    def append(self, df: DataFrame) -> None:
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
