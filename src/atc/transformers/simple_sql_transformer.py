import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.sql.SqlServer import SqlServer
from atc.utils import SelectAndCastColumns


class SimpleSqlServerTransformer(Transformer):
    def __init__(self, *, table_id: str, server: SqlServer, ignoreCase=False):
        super().__init__()
        self.server = server
        self.table_id = table_id
        self.ignoreCase = ignoreCase

    def process(self, df: DataFrame) -> DataFrame:
        # This transformation ensures that the selected columns
        # are casted to the target types
        # [f.col("ColumnName).cast("string").alias("ColumnName"), ...]

        df = SelectAndCastColumns(
            df=df,
            schema=self.server.read_table(self.table_id).schema,
            caseInsensitiveMatching=self.ignoreCase,
        )

        # If the format is timestamp, the seconds should be trunc
        col_choose = [
            f.date_trunc("second", f.col(x[0])).alias(x[0])
            if x[1] == "timestamp"
            else f.col(x[0])
            for x in df.dtypes
        ]
        df = df.select(col_choose)

        return df
