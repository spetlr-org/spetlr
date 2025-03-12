from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class ReplaceEmptyStringTransformer(Transformer):
    """
    This transformer looks at a dataframe,
    and if it contains empty strings replaces those with null values instead
    """

    def process(self, df: DataFrame) -> DataFrame:

        return df.replace("", None)
