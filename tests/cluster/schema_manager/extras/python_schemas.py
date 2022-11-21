from pyspark.sql import types as t

python_test_schema = t.StructType(
    [
        t.StructField("a", t.IntegerType(), True),
        t.StructField(
            "b",
            t.IntegerType(),
            True,
            metadata={"comment": "really? is that it?"},
        ),
        t.StructField("c", t.StringType(), True),
        t.StructField(
            "cplx",
            t.StructType(
                [
                    t.StructField("someId", t.StringType(), True),
                    t.StructField(
                        "details",
                        t.StructType([t.StructField("id", t.StringType(), True)]),
                        True,
                    ),
                    t.StructField("blabla", t.ArrayType(t.IntegerType(), True), True),
                ]
            ),
            True,
        ),
        t.StructField("d", t.TimestampType(), True),
        t.StructField("m", t.MapType(t.IntegerType(), t.StringType(), True), True),
        t.StructField("p", t.DecimalType(10, 3), True),
        t.StructField("final", t.StringType(), True),
    ]
)

python_test_schema2 = t.StructType(
    [
        t.StructField("a", t.IntegerType(), True),
        t.StructField("c", t.StringType(), True),
        t.StructField("d", t.TimestampType(), True),
        t.StructField("m", t.MapType(t.IntegerType(), t.StringType(), True), True),
        t.StructField("p", t.DecimalType(10, 3), True),
        t.StructField("final", t.StringType(), True),
    ]
)
