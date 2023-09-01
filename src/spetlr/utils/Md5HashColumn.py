from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5


def Md5HashColumn(
    df: DataFrame, colName: str, cols_to_exclude: List[str] = None
) -> DataFrame:
    """
    Generate column with md5 encoding, based on a list of column.

    it's optional to add a list of columns to ignore when generating the column

    concat_ws replace null values with an empty string

    param df: The pyspark DataFrame where the column is added
    param colName: Name for the column
    param cols_to_exclude: Optional list of columns to exclude

    return: DataFrame with md5 hash column
    """

    cols_to_exclude = cols_to_exclude or []

    cols = [col for col in df.columns if col not in cols_to_exclude]
    df = df.withColumn(colName, md5(concat_ws("||", *cols)))
    return df
