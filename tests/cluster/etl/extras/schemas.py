from pyspark.sql import types as t

test_base_schema = t.StructType(
    [
        t.StructField("integer", t.IntegerType(), True),
        t.StructField("string", t.StringType(), True),
    ]
)

test_col_type_difference_schema = t.StructType(
    [
        t.StructField("integer", t.DoubleType(), False),
        t.StructField("string", t.StringType(), False),
    ]
)

test_added_col_schema = t.StructType(
    [
        t.StructField("integer", t.IntegerType(), False),
        t.StructField("string", t.StringType(), False),
        t.StructField("double", t.DoubleType(), False),
    ]
)

test_removed_col_schema = t.StructType(
    [
        t.StructField("integer", t.IntegerType(), False),
    ]
)

test_renamed_col_schema = t.StructType(
    [
        t.StructField("int", t.IntegerType(), False),
        t.StructField("string", t.StringType(), False),
    ]
)
