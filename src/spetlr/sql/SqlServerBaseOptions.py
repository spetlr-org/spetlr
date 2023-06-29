import dataclasses


@dataclasses.dataclass
class SqlServerBaseOptions:
    jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    pyodbc_driver = "ODBC Driver 17 for SQL Server"
