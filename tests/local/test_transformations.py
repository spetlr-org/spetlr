import unittest
from datetime import datetime

import pyspark.sql.types as T

from atc.spark import Spark
from atc.transformations import join_time_series_dataframes


class TestFunctions(unittest.TestCase):
    def test_join_time_series_dataframes(self):
        spark = Spark.master("local[*]").get()

        df1Schema = T.StructType(
            [
                T.StructField("Id", T.LongType(), True),
                T.StructField("Name", T.StringType(), True),
                T.StructField("StartTimestamp", T.TimestampType(), True),
                T.StructField("EndTimestamp", T.TimestampType(), True),
                T.StructField("State", T.StringType(), True),
            ]
        )

        df1Data = [
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=0),
                datetime(year=2020, month=1, day=2, hour=0),
                "state1",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=0),
                datetime(year=2020, month=1, day=3, hour=0),
                "state2",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=3, hour=0),
                datetime(year=2020, month=1, day=4, hour=0),
                "state1",
            ),
        ]

        df2Schema = T.StructType(
            [
                T.StructField("Id", T.LongType(), True),
                T.StructField("StartTimestamp", T.TimestampType(), True),
                T.StructField("EndTimestamp", T.TimestampType(), True),
                T.StructField("State", T.StringType(), True),
            ]
        )

        df2Data = [
            (
                1,
                datetime(year=2020, month=1, day=1, hour=6),
                datetime(year=2020, month=1, day=1, hour=8),
                "state3",
            ),
            (
                1,
                datetime(year=2020, month=1, day=1, hour=10),
                datetime(year=2020, month=1, day=1, hour=12),
                "state3",
            ),
            (
                1,
                datetime(year=2020, month=1, day=1, hour=20),
                datetime(year=2020, month=1, day=2, hour=4),
                "state4",
            ),
            (
                1,
                datetime(year=2020, month=1, day=2, hour=6),
                datetime(year=2020, month=1, day=2, hour=8),
                "state3",
            ),
            (
                1,
                datetime(year=2020, month=1, day=2, hour=10),
                datetime(year=2020, month=1, day=2, hour=12),
                "state3",
            ),
        ]

        dfExpectedData = [
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=0),
                datetime(year=2020, month=1, day=1, hour=6),
                "state1",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=6),
                datetime(year=2020, month=1, day=1, hour=8),
                "state3",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=8),
                datetime(year=2020, month=1, day=1, hour=10),
                "state1",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=10),
                datetime(year=2020, month=1, day=1, hour=12),
                "state3",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=12),
                datetime(year=2020, month=1, day=1, hour=20),
                "state1",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=1, hour=20),
                datetime(year=2020, month=1, day=2, hour=4),
                "state4",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=4),
                datetime(year=2020, month=1, day=2, hour=6),
                "state2",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=6),
                datetime(year=2020, month=1, day=2, hour=8),
                "state3",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=8),
                datetime(year=2020, month=1, day=2, hour=10),
                "state2",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=10),
                datetime(year=2020, month=1, day=2, hour=12),
                "state3",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=2, hour=12),
                datetime(year=2020, month=1, day=3, hour=0),
                "state2",
            ),
            (
                1,
                "TestName",
                datetime(year=2020, month=1, day=3, hour=0),
                datetime(year=2020, month=1, day=4, hour=0),
                "state1",
            ),
        ]

        # Construct dataframes
        df1 = spark.createDataFrame(df1Data, df1Schema)
        df2 = spark.createDataFrame(df2Data, df2Schema)
        dfExpected = spark.createDataFrame(dfExpectedData, df1Schema)

        dfReturn = join_time_series_dataframes(
            dfPrimary=df1,
            dfSecondary=df2,
            startTimeColumnName="StartTimestamp",
            endTimeColumnName="EndTimestamp",
            idColumns=["Id"],
            stateColumn="State",
        )

        self.assertEqual(
            dfExpected.orderBy("StartTimestamp").collect(),
            dfReturn.orderBy("StartTimestamp").collect(),
        )


if __name__ == "__main__":
    unittest.main()
