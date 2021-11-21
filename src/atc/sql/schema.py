from pyspark.sql import types as t

from atc.spark import Spark


def get_schema(sql: str, spark=None) -> t.StructType:
    if spark is None:
        spark = Spark.get()

    return t._parse_datatype_string(
        sql
    )  # Turns out there was a standard function fo this all along :,(
