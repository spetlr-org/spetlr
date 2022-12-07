import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


def _lower(col: str, caseInsensitiveMatching: bool):
    return col.lower() if caseInsensitiveMatching else col


def SelectAndCastColumns(
    *, df: DataFrame, schema: T.StructType, caseInsensitiveMatching: bool = False
) -> DataFrame:
    """
    Select and cast columns based pyspark schema.
    In case columns from shema are missing, they will be added with None values.

    param df: The pyspark DataFrame.
    param schema: pyspark schema to use for select and cast.
    param caseInsensitiveMatching: Boolean used for case insensitive matching,
    by default False.

    return: DataFrame mathing schema
    """

    dfColumns = [_lower(col, caseInsensitiveMatching) for col in df.columns]

    selectAndCastColumnsList = [
        F.col(c.name).cast(c.dataType).alias(c.name)
        if _lower(c.name, caseInsensitiveMatching) in dfColumns
        else F.lit(None).cast(c.dataType).alias(c.name)
        for c in schema
    ]

    return df.select(selectAndCastColumnsList)
