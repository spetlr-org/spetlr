from atc.functions import init_dbutils
from atc.sql.SqlServer import SqlServer


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):

        self.hostname = "githubatctest.database.windows.net"
        self.username = init_dbutils().secrets.get("secrets", "DbDeploy--ClientId")
        self.password = init_dbutils().secrets.get("secrets", "DbDeploy--ClientSecret")
        self.database = "Delivery"
        self.port = "1433"
        self.is_spn = True
        super().__init__(
            self.hostname,
            self.database,
            self.username,
            self.password,
            self.port,
            connection_string=None,
            is_spn=self.is_spn,
        )
