import pyspark.sql.types as T

test_schema = T.StructType(
    [
        T.StructField("a", T.StringType(), True),
        T.StructField("b", T.IntegerType(), True),
        T.StructField("c", T.BooleanType(), True),
    ]
)
