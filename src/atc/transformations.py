import warnings
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

from atc.atc_exceptions import NoTableException
from atc.functions import get_unique_tempview_name
from atc.spark import Spark


def join_time_series_dataframes(
    dfPrimary: DataFrame,
    dfSecondary: DataFrame,
    startTimeColumnName: str,
    endTimeColumnName: str,
    idColumns: List[str],
    stateColumn: str,
) -> DataFrame:
    """
    Join changing states from two dataframes acording to time as follows.

    dfPri   dfSec   df
    --              --
    |               |
    |               s1
    |       --      --
    s1      s3      s3
    |       --      --
    |               s1
    |       --      --
    --      s4      s4
    |       --      --
    |               s2
    |       --      --
    s2      s3      s3
    |       --      --
    |               s2
    |               |
    --              --
            --
            s5
            --


    """

    # Rename columns to differentiate the two dataframes
    dfPrimary_join = dfPrimary
    for column in dfPrimary_join.columns:
        dfPrimary_join = dfPrimary_join.withColumnRenamed(column, f"dfPri_{column}")

    dfSecondary_join = dfSecondary
    for column in dfSecondary_join.columns:
        dfSecondary_join = dfSecondary_join.withColumnRenamed(column, f"dfSec_{column}")

    # Create join exception
    idMatchString = " AND ".join(
        [f"dfPri_{column} = dfSec_{column}" for column in idColumns]
    )
    timeMatchString = f"( \
            (dfSec_{startTimeColumnName} >= dfPri_{startTimeColumnName} \
                AND dfSec_{startTimeColumnName} < dfPri_{endTimeColumnName}) \
            OR (dfSec_{endTimeColumnName} >= dfPri_{startTimeColumnName} \
                AND dfSec_{endTimeColumnName} < dfPri_{endTimeColumnName})\
        )"
    joinExpr = F.expr(f"{idMatchString} AND {timeMatchString}")

    # Outer join the two dataframes acording to timestamp to get all matches over time
    df_join = dfPrimary_join.join(dfSecondary_join, on=joinExpr, how="leftouter")

    # Select data after join to remove duplicated id columns
    df_join = df_join.select(
        [F.col(f"dfPri_{column}").alias(column) for column in idColumns]
        + [
            f"dfPri_{column}"
            for column in [startTimeColumnName, endTimeColumnName, stateColumn]
        ]
        + [
            f"dfSec_{column}"
            for column in [startTimeColumnName, endTimeColumnName, stateColumn]
        ]
        + [
            F.col(f"dfPri_{column}").alias(column)
            for column in dfPrimary.columns
            if column
            not in [startTimeColumnName, endTimeColumnName, stateColumn] + idColumns
        ]
    )

    # Create state id to use for groupings because value can change back to same value
    window = Window.partitionBy(idColumns).orderBy(f"dfPri_{startTimeColumnName}")
    df_join = df_join.withColumn(
        "dfPri_StateGroupId",
        F.sum(
            (
                F.col(f"dfPri_{stateColumn}")
                != F.lag(F.col(f"dfPri_{stateColumn}")).over(window)
            ).cast("int")
        ).over(window),
    )
    df_join = df_join.fillna({"dfPri_StateGroupId": 0})

    # Fix start and end timestamps when multiple states in secondary dataframe
    # match primary dataframe
    window = Window.partitionBy(idColumns + ["dfPri_StateGroupId"]).orderBy(
        f"dfPri_{startTimeColumnName}", f"dfSec_{startTimeColumnName}"
    )
    # endtime
    df_join = df_join.withColumn(
        f"dfPri_{endTimeColumnName}",
        F.when(
            (
                F.col(f"dfPri_{endTimeColumnName}")
                == F.lead(F.col(f"dfPri_{endTimeColumnName}")).over(window)
            ),
            F.col(f"dfSec_{endTimeColumnName}"),
        ).otherwise(F.col(f"dfPri_{endTimeColumnName}")),
    )
    # start time
    df_join = df_join.withColumn(
        f"dfPri_{startTimeColumnName}",
        F.when(
            (
                F.col(f"dfPri_{startTimeColumnName}")
                == F.lag(F.col(f"dfPri_{startTimeColumnName}")).over(window)
            ),
            F.lag(F.col(f"dfSec_{endTimeColumnName}")).over(window),
        ).otherwise(F.col(f"dfPri_{startTimeColumnName}")),
    )

    # Generating three time segements:
    #   - One with the state from the primary dataframe
    #   - One with the state from the secondary dataframe
    #   - One with the rest of primary dataframe
    # Correcting the end timestamps according to if there is
    # a matching element in secondary dataframe
    df_join = df_join.withColumn(
        "StartTimeDfPri1", F.col(f"dfPri_{startTimeColumnName}")
    )
    df_join = df_join.withColumn(
        "EndTimeDfPri1",
        F.when(
            F.col(f"dfSec_{stateColumn}").isNotNull(),
            F.col(f"dfSec_{startTimeColumnName}"),
        ).otherwise(F.col(f"dfPri_{endTimeColumnName}")),
    )

    df_join = df_join.withColumn(
        "StartTimeDfSec", F.col(f"dfSec_{startTimeColumnName}")
    )
    df_join = df_join.withColumn("EndTimeDfSec", F.col(f"dfSec_{endTimeColumnName}"))

    df_join = df_join.withColumn("StartTimeDfPri2", F.col(f"dfSec_{endTimeColumnName}"))

    df_join = df_join.withColumn(
        "EndTimeDfPri2",
        F.when(
            F.col(f"dfSec_{stateColumn}").isNotNull(),
            F.col(f"dfPri_{endTimeColumnName}"),
        ).otherwise(None),
    )

    # Collecting segments of timestamps into three lists
    df_join = (
        df_join.withColumn(
            "DfPri1",
            F.when(
                F.col("StartTimeDfPri1") < F.col("EndTimeDfPri1"),
                F.array(F.col("StartTimeDfPri1"), F.col("EndTimeDfPri1")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
        .withColumn(
            "DfSec",
            F.when(
                F.col("StartTimeDfSec") < F.col("EndTimeDfSec"),
                F.array(F.col("StartTimeDfSec"), F.col("EndTimeDfSec")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
        .withColumn(
            "DfPri2",
            F.when(
                F.col("StartTimeDfPri2") < F.col("EndTimeDfPri2"),
                F.array(F.col("StartTimeDfPri2"), F.col("EndTimeDfPri2")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
    )

    # Unpivot dataframe to move timestamp segemnts from columns to rows
    unpivot_columnn_select = (
        idColumns
        + [
            f"{column}"
            for column in dfPrimary.columns
            if column
            not in [startTimeColumnName, endTimeColumnName, stateColumn] + idColumns
        ]
        + [
            f"dfPri_{stateColumn}",
            f"dfSec_{stateColumn}",
            "stack(3, 'DfPri1', DfPri1, 'DfSec', DfSec, 'DfPri2', DfPri2) "
            "as (type, data)",
        ]
    )
    df_join_unpivot = df_join.selectExpr(*unpivot_columnn_select)

    # Extract two timestamps from unpivot data column
    df_join_unpivot = (
        df_join_unpivot.withColumn(startTimeColumnName, F.col("data")[0])
        .withColumn(endTimeColumnName, F.col("data")[1])
        .drop("data")
    )

    # Remove rows where start time and end time is null
    df_join_unpivot = df_join_unpivot.where(
        F.col(startTimeColumnName).isNotNull() | F.col(endTimeColumnName).isNotNull()
    )

    # Define stateColumn from either df primary or df seconday
    df_join_unpivot = df_join_unpivot.withColumn(
        "tempStateColumn",
        F.when(F.col("type") == "DfPri1", F.col(f"dfPri_{stateColumn}"))
        .when(F.col("type") == "DfSec", F.col(f"dfSec_{stateColumn}"))
        .when(F.col("type") == "DfPri2", F.col(f"dfPri_{stateColumn}"))
        .otherwise(None),
    )
    df_join_unpivot = df_join_unpivot.drop(
        f"dfPri_{stateColumn}", f"dfSec_{stateColumn}", "type"
    )
    df_join_unpivot = df_join_unpivot.withColumnRenamed("tempStateColumn", stateColumn)

    df_join_unpivot = df_join_unpivot.dropDuplicates(
        subset=[startTimeColumnName, endTimeColumnName]
    )

    return df_join_unpivot


def merge_df_into_target(
    df: DataFrame,
    table_name: str,
    database_name: str,
    join_cols: List[str],
    table_format: str = "Delta",
) -> None:
    """

    Merges a databricks dataframe into a target database table

    :param df: The dataframe
    :param table_name: The name of the table which the dataframe (p1)
        should be merged into
    :param database_name: The database name associated with the table
    :param join_cols: A list of strings which tells which columns to join on
    :param table_format: The format of the table

    """

    target_table_name = str(database_name + "." + table_name)

    # Check if table exists
    if not Spark.get()._jsparkSession.catalog().tableExists(target_table_name):
        raise NoTableException(f"The table {target_table_name} not found.")

    # check null keys in our dataframe.
    any_null_keys = len(
        df.filter(" OR ".join(f"({col} is NULL)" for col in join_cols)).take(1)
    )

    if any_null_keys:
        warnings.warn(
            "Null keys found in input dataframe. Rows will be discarded before load."
        )
        df = df.filter(" AND ".join(f"({col} is NOT NULL)" for col in join_cols))

    df_target = Spark.get().table(target_table_name)

    # If the target is empty, always do faster full load
    if len(df_target.take(1)) == 0:
        return (
            df.write.format(table_format)
            .mode("overwrite")
            .saveAsTable(target_table_name)
        )  # Consider use .save instead

    # Find records that need to be updated in the target (happens seldom)

    # Define the column to be used for checking for new rows
    # Checking the null-ness of one right row is sufficient to mark the row as new,
    # since null keys are disallowed.
    key_col = join_cols[-1]

    # Write the filtering string including casting maps to strings
    filter_string = f"{key_col} IS NULL"
    for col, col_typ in df.dtypes:
        if col not in join_cols:
            if col_typ == "map":
                filter_string += (
                    f" OR (CAST(a.{col} AS varchar) <> CAST(b.{col} AS varchar))"
                )
            else:
                filter_string += f" OR (a.{col} <> b.{col})"
                filter_string += f" OR (a.{col} IS NULL AND b.{col} IS NOT NULL)"
                filter_string += f" OR (a.{col} IS NOT NULL AND b.{col} IS NULL)"

    df = (
        df.alias("a")
        .join(
            df_target.alias("b").select(
                "b.*", F.col(f"b.{key_col}").alias(f"{key_col}_copy")
            ),
            on=join_cols,
            how="left",
        )
        .filter(filter_string)
        .withColumn(
            "is_new",
            F.when(F.col(f"{key_col}_copy").isNull(), True).otherwise(False),
        )
        .select("a.*", "is_new")
        .cache()
    )

    # If merge is not required, the data can just be appended
    merge_required = len(df.filter(~F.col("is_new")).take(1)) > 0
    df = df.drop("is_new")

    if not merge_required:
        return (
            df.write.format(table_format).mode("append").saveAsTable(target_table_name)
        )  # Consider use .save instead

    temp_view_name = get_unique_tempview_name()
    df.createOrReplaceTempView(temp_view_name)

    merge_sql_statement = f"""
        MERGE INTO {target_table_name} AS target
        USING {temp_view_name} as source
        ON
        {" AND ".join(f"(source.{col} = target.{col})" for col in join_cols)}
        WHEN MATCHED
        THEN UPDATE -- update existing records
        SET
        *
        WHEN NOT MATCHED
        THEN INSERT -- insert new records
        *
        """
    Spark.get().sql(merge_sql_statement)


def concat_dfs(dfs: List[DataFrame]):
    """
    concat_dfs append dataframes on eachother and also append columns.
         If columns has the exact same name, they will be converted to one column.

    NB: The transformation does not perform dataframe joins.

    :param dfs: A list of spark dataframes

    returns a dataframe

    """

    # Check that the list of dataframe and each dataframe is not Nonetypes
    if dfs is None:
        raise NoTableException("The table list of tables are None")

    for i, df in enumerate(dfs):
        if df is None:
            raise NoTableException(f"The {i}th table dataframe in the list is None.")

    all_cols = []
    dfs_updated = dfs

    # Get all columns across all tables
    for df in dfs:
        all_cols = all_cols + df.columns

    # Sort the columns
    all_cols = sorted(list(set(all_cols)))

    # For each dataframe: Add the columns from all_cols if the dataframe does not has it
    for i, df in enumerate(dfs):
        for col in all_cols:
            if col not in df.columns:
                dfs_updated[i] = dfs_updated[i].withColumn(col, F.lit(None))

        # Select columns in correct order
        dfs_updated[i] = dfs_updated[i].select(all_cols)

    # Union the dataframes
    for i, df in enumerate(dfs):
        if i == 0:
            result = df
        else:
            result = result.unionByName(df)

    return result
