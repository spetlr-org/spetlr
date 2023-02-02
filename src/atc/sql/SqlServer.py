import contextlib
import importlib.resources
import re
import time
import uuid
from types import ModuleType
from typing import List, Union

import pyodbc
from pyspark.sql import DataFrame

from atc.configurator.configurator import Configurator
from atc.spark import Spark
from atc.sql.sql_handle import SqlHandle
from atc.utils import GetMergeStatement


class SqlServer:
    def __init__(
        self,
        hostname: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        port: str = "1433",
        connection_string: str = None,
        *,
        spnid: str = None,
        spnpassword: str = None,
    ):
        """Create object to interact with sql servers. Pass all but
        connection_string to connect via values or pass only the
        connection_string as a keyword param to connect via connection string"""
        if connection_string is not None:
            hostname, port, username, password, database = self.from_connection_string(
                connection_string
            )
        if not (hostname and database and port):
            raise ValueError("Hostname or database parameters missing.")

        self.timeout = 180  # 180 sec due to serverless
        self.sleep_time = 5  # Every 5 seconds the connection tries to be established

        jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        self.url = (
            f"jdbc:sqlserver://{hostname}:{port};"
            f"database={database};"
            f"queryTimeout=0;"
            f"loginTimeout={self.timeout};"
            f"driver={jdbc_driver};"
        )

        if spnpassword and spnid and not password and not username:
            # Use spn
            self.url += (
                f"AADSecurePrincipalId={spnid};"
                f"AADSecurePrincipalSecret={spnpassword};"
                f"encrypt=true;"
                f"trustServerCertificate=false;"
                f"hostNameInCertificate=*.database.windows.net;"
                f"authentication=ActiveDirectoryServicePrincipal"
            )

        elif password and username and not spnpassword and not spnid:
            # Use SQL admin
            self.url += f"user={username};password={password}"
        else:
            raise ValueError("Use either SPN or SQL user - never both")

        self.odbc = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={hostname};"
            f"DATABASE={database};"
            f"PORT={port};"
            f"UID={username or spnid};"
            f"PWD={password or spnpassword};"
            f"Connection Timeout={self.timeout}"
        )

        # If it is a SPN user, then it should use AD SPN authentication
        if spnid:
            self.odbc += ";Authentication=ActiveDirectoryServicePrincipal"

    @staticmethod
    def from_connection_string(connection_string):
        """Extracts values for connection to sql server, as described in:
        https://docs.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlconnection.connectionstring?view=dotnet-plat-ext-6.0#remarks
        """
        try:
            server_pattern = (
                r"(Server|Address|Data Source|Addr|Network Address)"
                r"=(tcp:|np:|lpc:)?(?P<server>[\w\d\.]+),?(?P<port>\d*);"
            )
            hostname = re.search(server_pattern, connection_string).group("server")
            port = re.search(server_pattern, connection_string).group("port")
            if port == "":
                port = "1433"

            user_pattern = r"(User ID|UID|User)=(?P<user>[^;]+);"
            username = re.search(user_pattern, connection_string).group("user")

            password_pattern = "(Password|PWD)=(?P<pwd>[^;]+);"
            password = re.search(password_pattern, connection_string).group("pwd")

            database_pattern = r"(Initial Catalog|Database)=(?P<db>[^;]+);"
            database = re.search(database_pattern, connection_string).group("db")
        except AttributeError:
            raise ValueError("Connection string does not conform to standard")

        return hostname, port, username, password, database

    @contextlib.contextmanager
    def connect_to_db(self):
        conn = pyodbc.connect(self.odbc)
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()

    def execute_sql(self, sql: str):
        self.test_odbc_connection()

        with self.connect_to_db() as conn:
            conn.execute(sql)

    def from_tc(self, id: str) -> SqlHandle:
        """This method allows an instance of SqlServer to be a drop in for the class
        DeltaHandle. One can call from_tc(id) on either to get a table handle."""
        tc = Configurator()
        return SqlHandle(name=tc.table_name(id), sql_server=self)

    get_handle = from_tc

    # Convenience class for align with Spark.get() method .sql.
    def sql(self, sql: str):
        self.execute_sql(sql)

    def load_sql(self, sql: str):
        self.test_odbc_connection()
        return Spark.get().read.jdbc(url=self.url, table=sql)

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
        self.test_odbc_connection()

        df_source.repartition(partition_count).write.format(
            "com.microsoft.sqlserver.jdbc.spark" if big_data_set else "jdbc"
        ).mode("append" if append else "overwrite").option(
            "schemaCheckEnabled", False
        ).option(
            # https://learn.microsoft.com/en-us/sql/connect/spark/connector?view=sql-server-ver16#supported-options
            # https://github.com/microsoft/sql-spark-connector/blob/master/README.md
            # Implements an insert with TABLOCK option to improve write performance
            "tableLock",
            True,
        ).option(
            "batchSize", batch_size
        ).option(
            "truncate", not append
        ).option(
            "url", self.url
        ).option(
            "dbtable", table_name
        ).save()

    def upsert_to_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        join_cols: List[str],
        filter_join_cols: bool = True,
        overwrite_if_target_is_empty: bool = True,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        if df_source is None:
            return None

        if filter_join_cols:
            df_source = df_source.filter(
                " AND ".join(f"({col} is NOT NULL)" for col in join_cols)
            )
            print(
                "Rows with NULL join keys found in input dataframe"
                " will be discarded before load."
            )

        if overwrite_if_target_is_empty:
            df_target = self.read_table_by_name(table_name=table_name)

            if len(df_target.take(1)) == 0:
                return self.write_table_by_name(
                    df_source=df_source,
                    table_name=table_name,
                    append=False,
                    big_data_set=big_data_set,
                    batch_size=batch_size,
                    partition_count=partition_count,
                )

        # Define name of temp stagning table
        # ## defines the table as a temp sql table
        staging_table_name = f"##{uuid.uuid4().hex}"

        with self.connect_to_db() as conn:
            # Create temp staging table for merge based source table
            conn.execute(
                f"""
                SELECT * INTO {staging_table_name}
                FROM {table_name} WHERE 1 = 0;
                """
            )

            self.write_table_by_name(
                df_source=df_source,
                table_name=staging_table_name,
                append=False,
                big_data_set=big_data_set,
                batch_size=batch_size,
                partition_count=partition_count,
            )

            mergeQuery = GetMergeStatement(
                merge_statement_type="sql",
                target_table_name=table_name,
                source_table_name=staging_table_name,
                join_cols=join_cols,
                insert_cols=df_source.columns,
                update_cols=df_source.columns,
            )

            conn.execute(mergeQuery)

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
                    "Database connection failed. "
                    f"Retries {retry_limit - retries} more time."
                )
                time.sleep(self.sleep_time)  # Sleeps for 5 seconds

        if retries >= retry_limit:
            raise pyodbc.OperationalError

    def execute_sql_file(
        self,
        resource_path: Union[str, ModuleType],
        sql_file: str,
        arguments: dict = None,
    ):
        for sql in self.get_sql_file(
            resource_path=resource_path, sql_file=sql_file, arguments=arguments
        ):
            self.execute_sql(sql)

    @staticmethod
    def table_name(table_id: str):
        return Configurator().table_name(table_id)

    def read_table(self, table_id: str):
        return self.read_table_by_name(SqlServer.table_name(table_id))

    def write_table(
        self,
        df_source: DataFrame,
        table_id: str,
        append: bool = False,
        big_data_set: bool = True,
        batch_size: int = 10 * 1024,
        partition_count: int = 60,
    ):
        self.write_table_by_name(
            df_source,
            SqlServer.table_name(table_id),
            append,
            big_data_set,
            batch_size,
            partition_count,
        )

    def truncate_table(self, table_id: str):
        table_name = SqlServer.table_name(table_id)
        self.execute_sql(f"TRUNCATE TABLE {table_name}")

    def drop_table(self, table_id: str):
        self.drop_table_by_name(SqlServer.table_name(table_id))

    def drop_view(self, table_id: str):
        self.drop_view_by_name(SqlServer.table_name(table_id))

    def get_sql_file(
        self,
        resource_path: Union[str, ModuleType],
        sql_file: str = None,
        arguments: dict = None,
    ):
        """
        Returns SQL statements from the specified .sql file.
        The return value is a generator.
        :param sql_file: name of the .sql file - if empty,
            all .sql files with names ending with "-create" are returned.
        :param resource_path: path to the .sql files.
        :param arguments: values of optional arguments to be replaced in the .sql files.
        :return: generator with SQL statements.
        """

        sql_arguments = Configurator().get_all_details()

        if arguments is not None:
            sql_arguments.update(arguments)
        for file_name in importlib.resources.contents(resource_path):
            if (
                (sql_file is None and file_name.endswith("-create.sql"))
                or (sql_file == "*" and file_name.endswith(".sql"))
                or (sql_file is not None and file_name == (sql_file + "-create.sql"))
                or (sql_file is not None and file_name == (sql_file + ".sql"))
            ):
                with importlib.resources.path(resource_path, file_name) as file_path:
                    with open(file_path) as file:
                        for sql in file.read().split(sep="-- COMMAND ----------"):
                            yield sql.format(**sql_arguments)
