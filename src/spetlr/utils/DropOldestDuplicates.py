from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def DropOldestDuplicates(
    *, df: DataFrame, cols: List[str], orderByColumn: str
) -> DataFrame:
    """
    Removes the oldest duplicates.

    In other words, if there is multiple duplicates, only the newest row remain.

    param df: The pyspark DataFrame with potential duplicates
    param cols: A list of column names that the window function is partitioned by
    param orderByColumn: The time formatted column to order by

    return: DataFrame with oldest duplicate rows removed

    """
    wnd = Window.partitionBy(cols).orderBy(df[orderByColumn].desc())
    df = df.withColumn("rowNum", f.row_number().over(wnd)).where(f.col("rowNum") == 1)
    df = df.drop("rowNum")
    return df
