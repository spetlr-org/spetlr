import warnings

from pyspark.sql.functions import md5, concat_ws, col
from spetlr.etl.transformer import Transformer




class CaptureHistoryTransformer(Transformer):
    """
       This class archives data changes from a source table, using a primary key.
       It identifies and captures new or modified records.
       For accurate comparison of all columns, ensure both schemas are identical.

    """
    def __init__(self, primary_key, cols_to_exclude: List[str] = None):
        self.primary_key = primary_key
        if cols_to_exclude is None:
            self.cols_to_exclude = []
        elif primary_key in cols_to_exclude:
            raise ValueError(f"The primary key '{primary_key}' should not be excluded.")
        else:
            self.cols_to_exclude = cols_to_exclude


    def _md5_hash(self, df: DataFrame, colName: str):
        cols = [col for col in df.columns if col not in self.cols_to_exclude]
        return df.withColumn(colName, md5(concat_ws("||", *cols)))

    def _warn_if_schema_differs(df1: DataFrame, df2: DataFrame):
        if df1.schema != df2.schema:
            warnings.warn("Schemas are not identical")

    def process_many(self, datasets: dataset_group) -> DataFrame:

        df_source = dataset["source"]
        df_target = dataset["target"]

        self._warn_if_schema_differs(df_source, df_target)

        # Add an md5 hash column to both DataFrames
        df_source = self._md5_hash(df_source, 'md5_source')
        df_target = self._md5_hash(df_target, 'md5_target')
        df_joined = df_source.join(df_target, self.primary_key, 'left_outer')

        # Filter out rows where the MD5 hash values are different or the target values are null
        df = df_joined.filter((df_joined.md5_source != df_joined.md5_target) | df_joined.md5_target.isNull())
        return df





