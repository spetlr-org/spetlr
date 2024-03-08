from typing import List

import pycountry
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from spetlr.etl import Transformer


def translate_country_to_alpha2(country_name: str) -> str:
    """
    Method to translate country names to alpha-2 codes
    Args:
        country_name: The name of the country
    Returns:
        The corresponding alpha-2 code or an empty string if an invalid name was given
    """
    country = pycountry.countries.get(name=country_name)
    if country:
        return country.alpha_2
    else:
        return ""


translateUDF = F.udf(lambda z: translate_country_to_alpha2(z), T.StringType())


class CountryToAlphaCodeTransformer(Transformer):
    def __init__(
        self,
        col_name: str,
        output_col_name: str = None,
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ) -> None:
        """
        A simple transformer to translate country names to alpha-2 codes

        Args:
            col_name: The name of the column with country names
            output_col_name: The name of the column to create,
            defaults to col_name if none is given
        """
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.col_name = col_name
        self.output_col_name = output_col_name or self.col_name

    def process(self, df: DataFrame) -> DataFrame:
        """
        Method to add column to passed dataframe 'df'
        Args:
            df: The dataframe to work on
        Returns:
            The dataframe with an added or overwritten column
        """
        df = df.withColumn(self.output_col_name, translateUDF(F.col(self.col_name)))
        return df
