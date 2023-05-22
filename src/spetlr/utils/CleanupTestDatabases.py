from spetlr.config_master import Configurator
from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.spark import Spark


def CleanupTestDatabases():
    """
    This function can be applied for removing test databases
    in the Databricks Environment.
    """

    c = Configurator()
    if not c.is_debug():
        raise OnlyUseInSpetlrDebugMode()

    id_extension = c.get("ID")

    dbs = Spark.get().sql("SHOW DATABASES").collect()
    for (db,) in dbs:
        if db.endswith(id_extension):
            print(f"Now deleting database {db}")
            Spark.get().sql(f"DROP DATABASE {db} CASCADE")
    print("Database cleanup done.")
