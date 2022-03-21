from atc.spark import Spark
from atc.sql.SqlServer import SqlServer


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


class DeliverySqlServer(SqlServer):
    def __init__(
        self,
        database: str = "Delivery",
        hostname: str = None,
        username: str = None,
        password: str = None,
        port: str = "1433",
    ):

        self.hostname = "atctest.database.windows.net" if hostname is None else hostname
        self.username = (
            get_dbutils(Spark.get()).secrets.get("atc", "SqlServer--DatabricksUser")
            if username is None
            else username
        )
        self.password = (
            get_dbutils(Spark.get()).secrets.get(
                "atc", "SqlServer--DatabricksUserPassword"
            )
            if password is None
            else password
        )
        self.database = database
        self.port = port
        super().__init__(
            self.hostname, self.database, self.username, self.password, self.port
        )
