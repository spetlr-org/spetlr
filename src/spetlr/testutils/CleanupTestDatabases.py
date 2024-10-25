from spetlr.config_master import Configurator
from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.spark import Spark


def CleanupTestDatabases(catalog: str = None):
    """
    This function can be applied for removing test databases
    in the Databricks Environment.
    """

    c = Configurator()
    if not c.is_debug():
        raise OnlyUseInSpetlrDebugMode()

    ID = c.get("ID")

    if catalog is None:
        sql = f"SHOW DATABASES LIKE '*{ID}'"
    else:
        sql = f"SHOW DATABASES IN {catalog} LIKE '*{ID}'"

    for (db,) in Spark.get().sql(sql).collect():
        if catalog:
            db = f"{catalog}.{db}"
        print(f"Now deleting database {db}")
        Spark.get().sql(f"DROP DATABASE {db} CASCADE")
    print("Database cleanup done.")
