import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.sql.SqlServer import SqlServer


class SimpleSqlServerTransformer(Transformer):
    def __init__(
        self,
        *,
        table_id: str,
        server: SqlServer,
    ):
        super().__init__()
        self.server = server
        self.table_id = table_id

    def process(self, df: DataFrame) -> DataFrame:
        # This transformation ensures that the selected columns
        # are casted to the target types
        # [f.col("ColumnName).cast("string").alias("ColumnName"), ...]

        target_df = self.server.read_table(self.table_id)
        col_choose = [f.col(x[0]).cast(x[1]).alias(x[0]) for x in target_df.dtypes]
        df = df.select(col_choose)

        return df
