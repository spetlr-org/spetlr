from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class CleanColumnNamesTransformer(Transformer):
    """
    Clean up columns names by removing special characters,
    trimming whitespaces and changing other whitespaces to underscores

    Parameters
    ----------
        exclude_columns : Potential columns to exclude
    """

    def __init__(
        self,
        exclude_columns: List[str] = None,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = False,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.exclude_columns = exclude_columns or []
        self.keep_chars = [" ", "_"]
        self.replace_chars = [" ", "-"]

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

        return df
