from atc.functions import init_dbutils
from atc.sql.SqlServer import SqlServer
from tests.cluster.values import resourceName


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):
        super().__init__(
            hostname=f"{resourceName()}.database.windows.net",
            database="Delivery",
            spnpassword=init_dbutils().secrets.get("secrets", "DbDeploy--ClientSecret"),
            spnid=init_dbutils().secrets.get("secrets", "DbDeploy--ClientId"),
        )
