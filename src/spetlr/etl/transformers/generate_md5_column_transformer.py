import uuid
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from spetlr.etl import Transformer


class GenerateMd5ColumnTransformer(Transformer):
    """
    This transformer generates a unique column with md5 encoding based on other columns.
    The transformer also handles if a value is NULL, by replacing it with empty string.

    Attributes:
    ----------
        col_name : String
            name of the column to put the md5 encoding
        col_list : List[str]
            list of columns to use for the md5 encoding
        dataset_input_keys : Union[str, List[str]]
            list of input dataset keys
        dataset_output_key : str
            output dataset key
    """

    def __init__(
        self,
        *,
        col_name: str,
        col_list: List[str],
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.col_name = col_name
        self.col_list = col_list

    def process(self, df: DataFrame) -> DataFrame:
        temp_col_list = []
        for col in self.col_list:
            # Place null safe value in temp column
            temp_col_name = str(uuid.uuid4())
            temp_col_list.append(temp_col_name)

            # Cast column to string
            # Coalesce with empty string in order to ensure md5 enablement
            _query = F.coalesce(F.col(col).cast(T.StringType()), F.lit(""))

            df = df.withColumn(temp_col_name, _query)

        df = df.withColumn(self.col_name, F.md5(F.concat(*temp_col_list)))

        df = df.drop(*temp_col_list)

        return df
