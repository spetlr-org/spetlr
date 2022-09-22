from atc.functions import init_dbutils
from atc.sql.SqlServer import SqlServer


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):
        super().__init__(
            hostname="githubatctest.database.windows.net",
            database="Delivery",
            spnpassword=init_dbutils().secrets.get("secrets", "DbDeploy--ClientSecret"),
            spnid=init_dbutils().secrets.get("secrets", "DbDeploy--ClientId"),
        )
