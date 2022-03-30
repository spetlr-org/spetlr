import unittest

from pyspark.sql.types import StructType

import atc


class TestFunctions(unittest.TestCase):
    def test_spark(self):
        spark = atc.spark.Spark.master("local[*]").get()

        # construct an empty dataframe with three rows
        df = spark.createDataFrame([(), (), ()], StructType())

        df = df.withColumn("uuid", atc.functions.uuid()).cache()
        df.show()

        value = df.take(1)[0][0]
        # since it is variable, we can only assert
        # that the string is formatted as a uuid4
        self.assertRegex(
            value, r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        )
