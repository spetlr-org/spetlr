import pyspark.sql.functions as f
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase
from spetlrtools.time import dt_utc

from spetlr.etl.transformers.validfromto_transformer import ValidFromToTransformer
from spetlr.spark import Spark


class ValidFromToTransformerTest(DataframeTestCase):
    def test_01(self):
        max_time = dt_utc(2262, 4, 11)
        data = [
            (1, "Fender", "Telecaster", 5, dt_utc(2021, 7, 1, 10)),
            # Duplicate when comparing the primary columns (first three) and the timecol
            (1, "Fender", "Telecaster", 5, dt_utc(2021, 7, 1, 10)),
            (1, "Fender", "Telecaster", 4, dt_utc(2021, 7, 1, 11)),
            (2, "Gibson", "Les Paul", 27, dt_utc(2021, 7, 1, 11)),
            (3, "Ibanez", "RG", 22, dt_utc(2021, 8, 1, 11)),
            (3, "Ibanez", "RG", 26, dt_utc(2021, 9, 1, 11)),
            (3, "Ibanez", "RG", 18, dt_utc(2021, 10, 1, 11)),
        ]

        expected_data = [
            [
                1,
                "Fender",
                "Telecaster",
                5,
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 11),
                False,
            ],
            [
                1,
                "Fender",
                "Telecaster",
                4,
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
            ],
            [
                2,
                "Gibson",
                "Les Paul",
                27,
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
            ],
            [
                3,
                "Ibanez",
                "RG",
                22,
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 9, 1, 11),
                False,
            ],
            [
                3,
                "Ibanez",
                "RG",
                26,
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 10, 1, 11),
                False,
            ],
            [
                3,
                "Ibanez",
                "RG",
                18,
                dt_utc(2021, 10, 1, 11),
                max_time,
                True,
            ],
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
            ValidFromToTransformer(
                time_col="timecolumn", wnd_cols=["id", "model", "brand"]
            )
            .process(df)
            .drop("timecolumn")
            .orderBy(f.col("ValidFrom").asc(), f.col("ValidTo").asc())
        )

        self.assertDataframeMatches(
            df2,
            None,
            expected_data=expected_data,
        )
