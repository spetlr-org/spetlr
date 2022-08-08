# Atc SQL documentation

SQL methods ovrview:

* [SQL Server Class](#sql-server-class)
* [SqlExecutor](#sqlexecutor)

## SQL Server Class
Lets say you have a Azure SQL server called *youservername* with an associated database *yourdatabase*. The username is explicitly defined here as *customusername* and the password as *[REDACTED]*. Variables related to security challenges like passwords are recommended to be located in e.g. [databricks secrets](https://docs.databricks.com/security/secrets/index.html). Here is a usage example:
 
```python
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


## SqlExecutor
This nice class can help parse and execute sql-files. It can be used for both executing spark and Azure sql queries.

*NB: The parser uses the semicolon-character (;) to split the queries. If the character is used for other purposes than closing a query, there might arise some executing issues. Please use the Spark.get().sql() or SqlServer.sql() instead.* 

In the example below the SqlExecutor is inherited, and your sql server is used (see [SQL Server Class](#sql-server-class)). Furthermore, provide the module of the sql-files which can be executed into the *base_module*-variable.  
 
```python
from atc.sql.SqlExecutor import SqlExecutor
from tests.cluster.sql import extras
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer

class DeliverySqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras, server=DeliverySqlServer())
```

If one need to execute sql queries in Databricks using Spark, there is no need for providing a server. By default, the class uses the atc Spark class. 
```python
class SparkSqlExecutor(SqlExecutor):
    def __init__(self):
        super().__init__(base_module=extras)
```

In the setup job, one could consider to create all delivery SQL tables:

```python
# In setup.py
def setup_production_tables():
    TableConfigurator().set_prod()
    SparkSqlExecutor().execute_sql_file("*")
    DeliverySqlExecutor().execute_sql_file("*")
```
