from spetlr.functions import init_dbutils
from spetlr.sql.SqlServer import SqlServer
from tests.cluster.values import resourceName


class DeliverySqlServerSpn(SqlServer):
    def __init__(
        self,
    ):
        super().__init__(
            hostname=f"{resourceName()}test.database.windows.net",
            database="Delivery",
            spnpassword=init_dbutils().secrets.get(
                "secrets", "Databricks--ClientSecret"
            ),
            spnid=init_dbutils().secrets.get("secrets", "Databricks--ClientId"),
        )
