import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class SimpleEventhubHandleTransformer(Transformer):
    """
    This class is a simple, and naive, way of taking your
    dataframe - and save it in a value column ready for
    writing using the EventhubHandle class
    """

    def process(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("value", f.struct(df.columns))
            .withColumn("value", f.to_json("value"))
            .selectExpr("CAST(value AS STRING)")
        )
