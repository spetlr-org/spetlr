import time

from spetlr import Configurator
from spetlr.spark import Spark

_time_spent = 0


def CleanupTestDatabases():
    """
    This function can be applied for removing test databases in the Databricks Environment.
    """

    c = Configurator()
    if not c.is_debug():
        raise AssertionError("Only call this if the configurator is in debug")

    start = time.time()

    id_extension = c.get_all_details()["ID"]

    dbs = Spark.get().sql("SHOW DATABASES").collect()
    for (db,) in dbs:
        if id_extension in db:
            print(f"Now deleting database {db}")
            Spark.get().sql(f"DROP DATABASE {db} CASCADE")
    print("Database cleanup done.")

    end = time.time()
    global _time_spent
    _time_spent += end - start
    print(f"CleanupTestDatabases total execution time so far: {_time_spent}")
