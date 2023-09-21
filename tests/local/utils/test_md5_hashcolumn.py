import unittest

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.spark import Spark
from spetlr.utils.Md5HashColumn import Md5HashColumn


class TestMd5HashColumn(unittest.TestCase):
    schema = StructType(
        [
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]
    )

    def test_md5_hash_column_with_null(self):
        data = [
            ("John", "Johnson", 30, None),
            ("Diana", "Doe", 25, "F"),
            (None, None, None, None),
        ]

        df = Spark.get().createDataFrame(data=data, schema=self.schema)

        # Testing the function
        hashed_df = Md5HashColumn(df, "hashed_column")

        # Check if hashed column is created
        self.assertIn("hashed_column", hashed_df.columns)

        # Check if the value is not null
        for row in hashed_df.collect():
            self.assertIsNotNone(row["hashed_column"])

    def test_md5_hash_column_with_exclusion(self):
        data = [
            ("John", "Johnson", 30, "M"),
        ]

        df = Spark.get().createDataFrame(data=data, schema=self.schema)

        # Apply Md5HashColumn function with 'age' column exclusion
        hashed_df = Md5HashColumn(df, "hashed_column", cols_to_exclude=["age"])

        # Check if hashed column is created
        self.assertIn("hashed_column", hashed_df.columns)

        # The hash should change if we exclude the age, so let's verify
        without_exclusion = Md5HashColumn(df, "hashed_without_exclusion")

        # Using collect to fetch the row and compare
        hashed_value = hashed_df.collect()[0]["hashed_column"]
        without_exclusion_value = without_exclusion.collect()[0][
            "hashed_without_exclusion"
        ]

        self.assertNotEqual(hashed_value, without_exclusion_value)
