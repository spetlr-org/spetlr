import warnings
from typing import List

from pyspark.sql import DataFrame

from spetlr.etl.transformer import Transformer
from spetlr.etl.types import dataset_group
from spetlr.utils.Md5HashColumn import Md5HashColumn


class DataChangeCaptureTransformer(Transformer):
    """
    This class archives data changes from a source table, using a primary key column.
    It identifies and captures new or modified records.
    For accurate comparison of all columns, ensure both schemas are identical.

    """

    def __init__(
        self,
        primary_key,
        df_source="source",
        df_target="target",
        cols_to_exclude: List[str] = None,
    ):
        self.primary_key = primary_key
        self.df_source = df_source
        self.df_target = df_target
        self.cols_to_exclude = cols_to_exclude or []
        if primary_key in self.cols_to_exclude:
            raise ValueError(f"The primary key '{primary_key}' should not be excluded.")

    def _warn_if_schema_differs(self, df1: DataFrame, df2: DataFrame):
        if df1.schema != df2.schema:
            warnings.warn(
                "Schemas are not identical, "
                "making the MD5 hash comparison potentially unreliable"
            )

    def process_many(self, dataset: dataset_group) -> DataFrame:
        df_source = dataset[self.df_source]
        df_target = dataset[self.df_target]

        self._warn_if_schema_differs(df_source, df_target)

        # Add an md5 hash column to both DataFrames
        df_source = Md5HashColumn(df_source, "md5_source", self.cols_to_exclude)
        df_target = Md5HashColumn(df_target, "md5_target", self.cols_to_exclude)
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
        ).drop("md5_source", "md5_target")
        return df
