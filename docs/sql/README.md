# Spetlr SQL documentation

SQL methods ovrview:

- [Spetlr SQL documentation](#spetlr-sql-documentation)
  - [SQL Server Class](#sql-server-class)
    - [SQL Upsert](#sql-upsert)
  - [SqlExecutor](#sqlexecutor)

## SQL Server Class
Let's say you have an Azure SQL server called *youservername* with an associated database *yourdatabase*. The username is explicitly defined here as *customusername* and the password as *[REDACTED]*. Variables related to security challenges like passwords are recommended to be located in e.g. [databricks secrets](https://docs.databricks.com/security/secrets/index.html). Here is a usage example:
 
```python
from spetlr.sql import SqlServer
class ExampleSqlServer(SqlServer):
    def __init__(
        self,
        database: str = "yourdatabase",
        hostname: str = None,
        username: str = None,
        password: str = None,
        port: str = "1433",
    ):

        self.hostname = "yourservername.database.windows.net" if hostname is None else hostname
        self.username = "customusername" if username is None else username
        
        # The password should of course be collected from e.g. databricks secrets
        self.password ="[REDACTED]" if password is None else password 
        
        self.database = database
        self.port = port
        super().__init__(
            self.hostname, self.database, self.username, self.password, self.port
        )
```
### Using SPN to connect

If you are using a Service Principal to create connection to the server/database, you should set the ```spnid = "yourspnid", spnpassword = "[REDACTED]"``` instead. The SqlServer class ensures to connect properly via the JDBC/ODBC drivers. Note, you must use either SQL user credentials or SPN credentials - never both. 

### SQL Upsert

The method upserts (updates or inserts) a databricks dataframe into a target sql table. 

``` python
def upsert_to_table_by_name(
        self,
        df_source: DataFrame,
        table_name: str,
        join_cols: List[str],
        filter_join_cols: bool = True,
        overwrite_if_target_is_empty: bool = True,
    ):
    ...
```

If 'filter_join_cols' is True, the dataframe will be filtered to not contain Null values in sql merge operation. This can be set to `False` to save compute if this check is already handled.

If 'overwrite_if_target_is_empty' is True, the first row of the target table is read and if empty the dataframe is overwritten to the table instead of doing a merge. This can be set to `False` to save compute if the programmer knows the target never will be empty.

Usage example:
``` python
sql_server.upsert_to_table_by_name(df_new, "tableName", ["Id"])

```

## SqlExecutor
This nice class can help parse and execute sql-files. It can be used for both executing 
spark and Azure sql queries.

*NB: The parser uses the semicolon-character (;) to split the queries. If the character 
is used for other purposes than closing a query, there might arise some executing 
issues. Please use the Spark.get().sql() or SqlServer.sql() instead.* 

In the example below the SqlExecutor is inherited, and your sql server is used 
(see [SQL Server Class](#sql-server-class)). Furthermore, provide the module of the 
sql-files which can be executed into the *base_module*-variable.  
 
```python
from tests.cluster.sql import extras
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer
from spetlr.sql import SqlExecutor

class DeliverySqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras, server=DeliverySqlServer())
```

If one need to execute sql queries in Databricks using Spark, there is no need for 
providing a server. By default, the class uses the spetlr Spark class. 
```python
from spetlr.sql import SqlExecutor

class SparkSqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras)
```

In the setup job, one could consider to create all delivery SQL tables:

```python
from spetlr.configurator import Configurator
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor
from tests.cluster.sql.DeliverySqlExecutor import DeliverySqlExecutor

# In setup.py
def setup_production_tables():
    Configurator().set_prod()
    SparkSqlExecutor().execute_sql_file("*")
    DeliverySqlExecutor().execute_sql_file("*")
```

It can be useful to have sql files among the resources that are only applied in special
cases. The `SqlExecutor` therefore allows to exclude these files with the 
`exclude_pattern` parameter. Any name where this pattern is found in the name will not 
be included in the execution.
