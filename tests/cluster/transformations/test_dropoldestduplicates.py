from atc_tools.testing import DataframeTestCase
from atc_tools.time import dt_utc
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from atc.spark import Spark
from atc.transformers.drop_oldest_duplicate_transformer import (
    DropOldestDuplicatesTransformer,
)


class TestDropOldestDuplicatesTransformer(DataframeTestCase):
    def test_drop_oldest_duplicates_transformer(self):
        data = [
            (1, "Fender", "Telecaster", 5, dt_utc(2021, 7, 1, 10)),
            (1, "Fender", "Telecaster", 4, dt_utc(2021, 7, 1, 11)),
            (2, "Gibson", "Les Paul", 27, dt_utc(2021, 7, 1, 11)),
            (3, "Ibanez", "RG", 22, dt_utc(2021, 8, 1, 11)),
            (3, "Ibanez", "RG", 26, dt_utc(2021, 9, 1, 11)),
            (3, "Ibanez", "RG", 18, dt_utc(2021, 10, 1, 11)),
        ]

        expected_data = [
            [1, "Fender", "Telecaster", 4, dt_utc(2021, 7, 1, 11)],
            [2, "Gibson", "Les Paul", 27, dt_utc(2021, 7, 1, 11)],
            [3, "Ibanez", "RG", 18, dt_utc(2021, 10, 1, 11)],
        ]

        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("model", StringType(), True),
                StructField("brand", StringType(), True),
                StructField("amount", IntegerType(), True),
                StructField("timecolumn", TimestampType(), True),
            ]
        )

        df = Spark.get().createDataFrame(data=data, schema=schema)

        df2 = (
            DropOldestDuplicatesTransformer(
                cols=["id", "model", "brand"], orderByColumn="timecolumn"
            )
            .process(df)
            .orderBy("id")
        )

        self.assertDataframeMatches(
            df2,
            None,
            expected_data=expected_data,
        )
