import unittest

from pyspark.sql.types import StructType

import spetlr


class TestFunctions(unittest.TestCase):
    def test_spark(self):
        spark = spetlr.spark.Spark.master("local[*]").get()

        # construct an empty dataframe with three rows
        df = spark.createDataFrame([(), (), ()], StructType())

        df = df.withColumn("uuid", spetlr.functions.uuid()).cache()
        df.show()

        value = df.take(1)[0][0]
        # since it is variable, we can only assert
        # that the string is formatted as a uuid4
        self.assertRegex(
            value, r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        )
