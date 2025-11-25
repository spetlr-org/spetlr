from typing import List, Literal, Optional

import pyspark.sql.functions as f
from pyspark.sql import DataFrame


class RowHash:
    """
    Class for generating a row hash.
    """

    def __init__(
        self,
        algorithm: Literal["hash", "md5", "sha1", "sha2", "xxhash64"] = "hash",
        separator: str | None = None,
    ):
        """
        Initialize the RowHash with default algorithm and separator.

        param algorithm: Hash algorithm to use (e.g., "hash", "md5", "sha1", etc.)
        param separator: Separator to use when concatenating columns.

        """

        if separator is None and algorithm.lower() != "hash":
            raise ValueError("Separator must be provided for non-hash algorithms.")

        self.algorithm = algorithm
        self.separator = separator

    def __call__(
        self,
        df: DataFrame,
        colName: str,
        cols_to_include: Optional[List[str]] = None,
        cols_to_exclude: Optional[List[str]] = None,
    ) -> DataFrame:
        """
        Generate column with configurable hash encoding, based on a list of columns.
        By default the hash is computed over all columns.

        param df: The pyspark DataFrame where the hash column is added
        param colName: Name for the hash column
        param cols_to_include: List of columns to include when computing the row hash
        param cols_to_exclude: List of columns to exclude from the row hash calculation

        return: DataFrame with hash column
        """

        cols_to_exclude = cols_to_exclude or []
        cols_to_include = cols_to_include or df.columns

        cols = [col for col in cols_to_include if col not in cols_to_exclude]

        algorithm_name = self.algorithm.lower()

        # Special handling for "hash"-algorithm. It doesn't compute the hash from a
        # concatenated string. But it takes multiple columns directly
        if algorithm_name == "hash":
            hash_func = f.hash(*cols)
        else:
            concatenated = f.concat_ws(self.separator, *cols)

            # Try to get the hash function dynamically from pyspark.sql.functions
            if hasattr(f, algorithm_name):
                hash_function = getattr(f, algorithm_name)
                hash_func = hash_function(concatenated)
            else:
                raise ValueError(
                    (
                        f"Unsupported hash algorithm: {self.algorithm}. "
                        "Check if it's available in pyspark.sql.functions"
                    )
                )

        df = df.withColumn(colName, hash_func)
        return df
