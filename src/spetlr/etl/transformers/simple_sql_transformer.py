from datetime import datetime, timezone
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import TimestampType

from spetlr.etl import Transformer
from spetlr.sql.SqlServer import SqlServer
from spetlr.utils import SelectAndCastColumns


class SimpleSqlServerTransformer(Transformer):
    def __init__(
        self,
        *,
        table_id: str,
        server: SqlServer,
        ignoreCase=False,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.server = server
        self.table_id = table_id
        self.ignoreCase = ignoreCase
        self.min_time = datetime(1900, 1, 1, 0, 0, 1, tzinfo=timezone.utc)

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
            (
                f.date_trunc("second", f.col(x[0])).alias(x[0])
                if x[1] == "timestamp"
                else f.col(x[0])
            )
            for x in df.dtypes
        ]
        df = df.select(col_choose)

        # The sql server min date is 1753-01-01. If a timestamp column of the
        # dataframe has a date value lower than this min date, it will not be
        # possible to load the data to the sql server. Here, we check all columns
        # of the dataframe that are the timestamp type, and convert values of
        # those columns to our defined min date, if values are smaller than this date.
        for column in df.columns:
            if df.schema[column].dataType == TimestampType():
                df = df.withColumn(
                    column,
                    f.when(
                        f.col(column) < f.lit(self.min_time), f.lit(self.min_time)
                    ).otherwise(f.col(column)),
                )

        return df
