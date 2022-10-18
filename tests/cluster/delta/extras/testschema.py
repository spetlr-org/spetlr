import pyspark.sql.types as T

test_schema = T.StructType(
    [
        T.StructField("TestCol1", T.IntegerType(), True),
        T.StructField("TestCol2", T.StringType(), False),
        T.StructField("TestCol3", T.TimestampType(), True),
        T.StructField("TestCol4", T.BooleanType(), True),
    ]
)
