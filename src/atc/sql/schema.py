import deprecation
from pyspark.sql import types as t

import atc
from atc.spark import Spark


@deprecation.deprecated(
    deprecated_in="0.3.3",
    removed_in="0.4",
    current_version=atc.__version__,
    details="Use pyspark.sql.types._parse_datatype_string instead.",
)
def get_schema(sql: str, spark=None) -> t.StructType:
    if spark is None:
        spark = Spark.get()

    return t._parse_datatype_string(
        sql
    )  # Turns out there was a standard function fo this all along :,(
