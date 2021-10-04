from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window

from typing import List


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
    for column in dfPrimary.columns:
        dfPrimary = dfPrimary.withColumnRenamed(column, f"dfPri_{column}")

    for column in dfSecondary.columns:
        dfSecondary = dfSecondary.withColumnRenamed(column, f"dfSec_{column}")

    # Create join expection
    idMatchString = " AND ".join([f"dfPri_{column} = dfSec_{column}" for column in idColumns])
    timeMatchString = (
        f"( \
            (dfSec_{startTimeColumnName} >= dfPri_{startTimeColumnName} \
                AND dfSec_{startTimeColumnName} < dfPri_{endTimeColumnName}) \
            OR (dfSec_{endTimeColumnName} >= dfPri_{startTimeColumnName} \
                AND dfSec_{endTimeColumnName} < dfPri_{endTimeColumnName})\
        )"
    )
    joinExpr = F.expr(f"{idMatchString} AND {timeMatchString}")

    # Outer join the two dataframes acording to timestamp to get all matches over time
    df_join = dfPrimary.join(dfSecondary, on=joinExpr, how="leftouter")

    # Select data after join to remove duplicated id columns
    df_join = df_join.select(
        [F.col(f"dfPri_{column}").alias(column) for column in idColumns]
        + [f"dfPri_{column}" for column in [startTimeColumnName, endTimeColumnName, stateColumn]]
        + [f"dfSec_{column}" for column in [startTimeColumnName, endTimeColumnName, stateColumn]]
    )

    # Create state id to use for groupings because value can change back to same value
    window = Window.partitionBy(idColumns).orderBy(f"dfPri_{startTimeColumnName}")
    df_join = df_join.withColumn(
        "dfPri_StateGroupId",
        F.sum(
            (
                F.col(f"dfPri_{stateColumn}") != F.lag(F.col(f"dfPri_{stateColumn}")).over(window)
            ).cast("int")
        ).over(window),
    )
    df_join = df_join.fillna({"dfPri_StateGroupId": 0})

    # Fix start and end timestamps when multiple states in secondary dataframe match primary dataframe
    window = Window.partitionBy(idColumns + ["dfPri_StateGroupId"]).orderBy(f"dfPri_{startTimeColumnName}", f"dfSec_{startTimeColumnName}")
        #endtime
    df_join = df_join.withColumn(
        f"dfPri_{endTimeColumnName}",
        F.when(
            (F.col(f"dfPri_{endTimeColumnName}") == F.lead(F.col(f"dfPri_{endTimeColumnName}")).over(window)),
            F.col(f"dfSec_{endTimeColumnName}"),
        ).otherwise(F.col(f"dfPri_{endTimeColumnName}")),
    )
        #start time
    df_join = df_join.withColumn(
        f"dfPri_{startTimeColumnName}",
        F.when(
            (F.col(f"dfPri_{startTimeColumnName}") == F.lag(F.col(f"dfPri_{startTimeColumnName}")).over(window)),
            F.lag(F.col(f"dfSec_{endTimeColumnName}")).over(window),
        ).otherwise(F.col(f"dfPri_{startTimeColumnName}")),
    )

    # Generating three time segements: 
    #   - One with the state from the primary dataframe
    #   - One with the state from the secondary dataframe 
    #   - One with the rest of primary dataframe
    # Correcting the end timestamps according to if there is a matching element in secondary dataframe
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
    df_join = df_join.withColumn(
        "EndTimeDfSec", F.col(f"dfSec_{endTimeColumnName}")
        )

    df_join = df_join.withColumn(
        "StartTimeDfPri2", F.col(f"dfSec_{endTimeColumnName}")
        )
        
    df_join = df_join.withColumn(
        "EndTimeDfPri2",
        F.when(
            F.col(f"dfSec_{stateColumn}").isNotNull(),
            F.col(f"dfPri_{endTimeColumnName}"),
        ).otherwise(None),
    )

    # Collecting segments of timestamps into three lists 
    df_join = (df_join.
        withColumn("DfPri1",
            F.when(
                F.col("StartTimeDfPri1") < F.col("EndTimeDfPri1"),
                F.array(F.col("StartTimeDfPri1"), F.col("EndTimeDfPri1")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
        .withColumn("DfSec",
            F.when(
                F.col("StartTimeDfSec") < F.col("EndTimeDfSec"),
                F.array(F.col("StartTimeDfSec"), F.col("EndTimeDfSec")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
        .withColumn("DfPri2",
            F.when(
                F.col("StartTimeDfPri2") < F.col("EndTimeDfPri2"),
                F.array(F.col("StartTimeDfPri2"), F.col("EndTimeDfPri2")),
            ).otherwise(F.array(F.lit(None), F.lit(None))),
        )
    )

    # Unpivot dataframe to move timestamp segemnts from columns to rows
    unpivot_columnn_select = (
        idColumns + [f"dfPri_{stateColumn}", f"dfSec_{stateColumn}", "stack(3, 'DfPri1', DfPri1, 'DfSec', DfSec, 'DfPri2', DfPri2) as (type, data)"]
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
        .otherwise(None)
    )
    df_join_unpivot = df_join_unpivot.drop(f"dfPri_{stateColumn}", f"dfSec_{stateColumn}", "type")
    df_join_unpivot = df_join_unpivot.withColumnRenamed("tempStateColumn", stateColumn)

    df_join_unpivot = df_join_unpivot.dropDuplicates(
        subset=[startTimeColumnName, endTimeColumnName]
    )

    return df_join_unpivot
