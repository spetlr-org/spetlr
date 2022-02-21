import unittest

from atc.spark import Spark


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except ImportError:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


# This file should test the function "drop_table_cascade" in atc/functions
class MergeDfIntoTargetTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_on_cluster(self):
        print("secret scopes", get_dbutils(Spark.get()).secrets.listScopes())
        self.assertTrue(True)
