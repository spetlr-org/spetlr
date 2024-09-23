from spetlr.functions import init_dbutils
from spetlr.sql.SqlServer import SqlServer
from tests.cluster.values import resourceName


class DeliverySqlServer(SqlServer):
    def __init__(
        self,
        database: str = "Delivery",
        hostname: str = None,
        username: str = None,
        password: str = None,
        port: str = "1433",
    ):
        self.hostname = (
            f"{resourceName()}test.database.windows.net"
            if hostname is None
            else hostname
        )
        self.username = (
            init_dbutils().secrets.get("secrets", "SqlServer--DatabricksUser")
            if username is None
            else username
        )
        self.password = (
            init_dbutils().secrets.get("secrets", "SqlServer--DatabricksUserPassword")
            if password is None
            else password
        )
        self.database = database
        self.port = port
        super().__init__(
            self.hostname, self.database, self.username, self.password, self.port
        )
