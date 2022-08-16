from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def CheckDfMerge(
    *,
    df: DataFrame,
    df_target: DataFrame,
    join_cols: List[str],
    avoid_cols: List[str],
):
    """This logic optimizes data load of dataframe, df, into a target table, df_target.
    it checks whether a merge is needed.
    If it is not needed, a simple insert is executed.
    """

    key_col = join_cols[-1]

    # Write the filtering string including casting maps to strings
    filter_string = f"b.{key_col} IS NULL"
    for col, col_typ in df.dtypes:
        if col not in join_cols + avoid_cols:
            if col_typ == "map":
                filter_string += (
                    f" OR (CAST(a.{col} AS varchar) <> CAST(b.{col} AS varchar))"
                )
            else:
                filter_string += f" OR (a.{col} <> b.{col})"
                filter_string += f" OR (a.{col} IS NULL AND b.{col} IS NOT NULL)"
                filter_string += f" OR (a.{col} IS NOT NULL AND b.{col} IS NULL)"

    # The following compares the df and df_target to check if any merging is required.
    #
    df = (
        df.alias("a")
        .join(
            df_target.alias("b").select(
                "b.*", f.col(f"b.{key_col}").alias(f"{key_col}_copy")
            ),
            on=join_cols,
            how="left",
        )
        .filter(filter_string)
        .withColumn(
            "is_new",
            f.when(f.col(f"{key_col}_copy").isNull(), True).otherwise(False),
        )
        .select("a.*", "is_new")
        .cache()
    )

    # merge_required == True if the data contains updates or deletes,
    # False if only inserts
    # note: inserts alone happen almost always and can use "append",
    # which is much faster than merge
    merge_required = len(df.filter(~f.col("is_new")).take(1)) > 0
    df = df.drop("is_new")

    return df, merge_required
