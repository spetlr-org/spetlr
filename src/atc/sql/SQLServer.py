import time

import pyodbc
from pyspark.sql import DataFrame

from atc.spark import Spark


class SqlServer:
    def __init__(
        self,
        hostname: str,
        database: str,
        username: str,
        password: str,
        port: str = "1433",
    ):
        self.timeout = 180  # 180 sec due to serverless
        self.sleep_time = 5  # Every 5 seconds the connection tries to be established
        self.url = f"jdbc:sqlserver://{hostname}:{port};database={database};queryTimeout=0;loginTimeout={self.timeout}"

        self.username = username
        self.password = password

        self.odbc = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={hostname};"
            f"DATABASE={database};"
            f"PORT={port};"
            f"UID={username};"
            f"PWD={password};"
            f"Connection Timeout={self.timeout}"
        )
        self.properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

    def execute_sql(self, sql: str):
        self.test_odbc_connection()
        conn = pyodbc.connect(self.odbc)
        conn.autocommit = True
        conn.execute(sql)

    def load_sql(self, sql: str):
        self.test_odbc_connection()
        return Spark.get().read.jdbc(
            url=self.url, table=sql, properties=self.properties
        )

    def read_table_by_name(self, table_name: str):
        return self.load_sql(f"(SELECT * FROM {table_name}) target")

    def write_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        append: bool = False,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        df_source.repartition(partition_count).write.format(
            "com.microsoft.sqlserver.jdbc.spark" if big_data_set else "jdbc"
        ).mode("append" if append else "overwrite").option(
            "schemaCheckEnabled", False
        ).option(
            "batchSize", batch_size
        ).option(
            "truncate", not append
        ).option(
            "url", self.url
        ).option(
            "dbtable", table_name
        ).option(
            "user", self.username
        ).option(
            "password", self.password
        ).save()

    def truncate_table_by_name(self, table_name: str):
        self.execute_sql(f"TRUNCATE TABLE {table_name}")

    def drop_table_by_name(self, table_name: str):
        self.execute_sql(
            f"""
                IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
                BEGIN
                  DROP TABLE {table_name}
                END
            """
        )

    def drop_view_by_name(self, table_name: str):
        self.execute_sql(
            f"""
                IF OBJECT_ID('{table_name}', 'V') IS NOT NULL
                BEGIN
                    DROP view {table_name}
                END
            """
        )

    def test_odbc_connection(self):
        """
        This function is introduced for handling serverless database automatic pausing.
        It tries to reconnect to the database if no connection is established.

        :return:
        prints if connection failed
        """
        connected = False
        retry_limit = self.timeout // self.sleep_time
        retries = 0  # Keeps track of the number of retries

        while not connected and retries < retry_limit:
            retries = retries + 1
            try:
                cnxn = pyodbc.connect(self.odbc)
                cnxn.close()
                connected = True  # Database connection success!
            except pyodbc.OperationalError:
                print(
                    f"Database connection failed. Retries {retry_limit - retries} more time."
                )
                time.sleep(self.sleep_time)  # Sleeps for 5 seconds

        if retries >= retry_limit:
            raise pyodbc.OperationalError

    def execute_sql_file(
        self, sql_file: str, resource_path: str, arguments: dict = None
    ):
        raise NotImplementedError("Waiting for configreader implementation...")

    #    for sql in ConfigReader().get_sql_file(sql_file, resource_path, arguments):
    #        self.execute_sql(sql)

    @staticmethod
    def table_name(table_id: str):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     return ConfigReader().table_name(table_id)

    def read_table(self, table_id: str):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     return self.read_table_by_name(SqlServer.table_name(table_id))

    def write_table(
        self,
        df_source: DataFrame,
        table_id: str,
        append: bool = False,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     self.write_table_by_name(
    #         df_source,
    #         SqlServer.table_name(table_id),
    #         append,
    #         big_data_set,
    #         batch_size,
    #         partition_count,
    #     )

    def truncate_table(self, table_id: str):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     table_name = SqlServer.table_name(table_id)
    #     self.execute_sql(f"TRUNCATE TABLE {table_name}")

    def drop_table(self, table_id: str):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     self.drop_table_by_name(SqlServer.table_name(table_id))

    def drop_view(self, table_id: str):
        raise NotImplementedError("Waiting for configreader implementation...")

    #     self.drop_view_by_name(SqlServer.table_name(table_id))
