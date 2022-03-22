# Atc SQL documentation

SQL methods ovrview:

* Documentation coming...
* ...

## SQL Class
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
