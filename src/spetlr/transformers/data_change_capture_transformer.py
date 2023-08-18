import warnings
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, md5

from spetlr.etl.transformer import Transformer
from spetlr.etl.types import dataset_group


class DataChangeCaptureTransformer(Transformer):
    """
    This class archives data changes from a source table, using a primary key column.
    It identifies and captures new or modified records.
    For accurate comparison of all columns, ensure both schemas are identical.

    """

    def __init__(
        self, primary_key, df_source, df_target, cols_to_exclude: List[str] = None
    ):
        self.primary_key = primary_key
        self.df_source = df_source
        self.df_target = df_target
        self.cols_to_exclude = cols_to_exclude or []
        if primary_key in cols_to_exclude:
            raise ValueError(f"The primary key '{primary_key}' should not be excluded.")

    def _md5_hash(self, df: DataFrame, colName: str):
        cols = [col for col in df.columns if col not in self.cols_to_exclude]
        return df.withColumn(colName, md5(concat_ws("||", *cols)))

    def _warn_if_schema_differs(self, df1: DataFrame, df2: DataFrame):
        if df1.schema != df2.schema:
            warnings.warn("Schemas are not identical")

    def process_many(self, dataset: dataset_group) -> DataFrame:
        df_source = self.df_source
        df_target = self.df_source

        self._warn_if_schema_differs(df_source, df_target)

        # Add an md5 hash column to both DataFrames
        df_source = self._md5_hash(df_source, "md5_source")
        df_target = self._md5_hash(df_target, "md5_target")
        df_joined = df_source.join(
            df_target.select("md5_target", self.primary_key),
            self.primary_key,
            "left_outer",
        )

        # Filter to keep rows where the MD5 hash values are different
        # or the target values are null
        df = df_joined.filter(
            (df_joined.md5_source != df_joined.md5_target)
            | df_joined.md5_target.isNull()
        )
        return df
