from typing import List

import pyspark.sql.types as T
from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.utils import SelectAndCastColumns


class SelectAndCastColumnsTransformer(Transformer):
    """
    This transformer select and cast columns based pyspark schema.

    Attributes:
    ----------
        schema : StructType
            pyspark schema to use for select and cast
        caseInsensitiveMatching : Boolean
            used for case insensitive matching, by default False
        dataset_input_keys : Union[str, List[str]]
            list of input dataset keys
        dataset_output_key : str
            output dataset key
    """

    def __init__(
        self,
        *,
        schema: T.StructType,
        caseInsensitiveMatching: bool = False,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.schema = schema
        self.caseInsensitiveMatching = caseInsensitiveMatching

    def process(self, df: DataFrame) -> DataFrame:
        return SelectAndCastColumns(
            df=df,
            schema=self.schema,
            caseInsensitiveMatching=self.caseInsensitiveMatching,
        )
