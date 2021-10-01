from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from typing import List

def create_id_match_string(idColumns: List[str]) -> str:
    return " AND ".join([f"dfPrimary.{column} = dfSecondary.{column}" for column in idColumns])

def join_time_seperated_dataframes(dfPrimary: DataFrame, dfSecondary: DataFrame, startTimeColumnName: str, endTimeColumnName: str, idColumns: List[str]):

    dfPrimary_join = dfPrimary.alias("dfPrimary")
    dfSecondary_join = dfSecondary.alias("dfSecondary")
    
    idMatchList = " AND ".join([f"dfPrimary.{column} = dfSecondary.{column}" for column in idColumns])

    joinExpr = F.expr("""MouldId = InactiveMouldId AND ((InactiveStartTime >= StartTime AND InactiveStartTime < EndTime) OR (InactiveEndTime >= StartTime AND InactiveEndTime < EndTime))""")

    df_join = df_taskcreatedevents_join.join(df_order_join, on=joinExpr, how="leftouter").drop("InactiveMouldId")
