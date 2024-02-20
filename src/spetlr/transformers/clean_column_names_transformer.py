from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.exceptions import AmbiguousColumnsAfterCleaning


class CleanColumnNamesTransformer(Transformer):
    """
    Clean up columns names by removing special characters,
    trimming whitespaces and changing other whitespaces to underscores

    NB: This transformer throws exception if ambiguous column names are created

    Parameters
    ----------
        exclude_columns : Potential columns to exclude
    """

    def __init__(
        self,
        exclude_columns: List[str] = None,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.exclude_columns = exclude_columns or []
        self.keep_chars = [" ", "_", "-"]  # Special characters not removed
        self.replace_chars = [" ", "-"]  # Characters that are replaced with "_"

    def process(self, df: DataFrame) -> DataFrame:
        columns = [col for col in df.columns if col not in self.exclude_columns]
        for col in columns:
            new_col_name = "".join(
                e for e in col if e.isalnum() or e in self.keep_chars
            )
            new_col_name = new_col_name.strip()
            for c in self.replace_chars:
                new_col_name = new_col_name.replace(c, "_")
            df = df.withColumnRenamed(col, new_col_name)

        if len(set(df.columns)) != len(df.columns):
            raise AmbiguousColumnsAfterCleaning(
                "Column names after cleaning are ambiguous!"
            )

        return df
