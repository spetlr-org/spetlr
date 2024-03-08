from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from timezonefinder import TimezoneFinder

from spetlr.etl import Transformer
from spetlr.etl.types import dataset_group
from spetlr.exceptions import ColumnDoesNotExistException


class TimeZoneTransformer(Transformer):
    """
    This transformer extracts a timezone using longitude and latitude.

    Attributes:
    ----------
        latitude_col : str
            name of column with latitude values
        longitude_col : str
            name of column with longitude values
        dataset_input_key : str
            input dataset identifier
        dataset_output_key : str
            output dataset identifier
        column_output_name : str
            output column name of the TimeZone column

    Methods
    -------
    process(df: DataFrame):
        returns the input DataFrame with a TimeZone column
    """

    def __init__(
        self,
        *,
        latitude_col: str,
        longitude_col: str,
        column_output_name: str = "TimeZone",
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.latitude_col = latitude_col
        self.longitude_col = longitude_col
        self.column_output_name = column_output_name

    def process(self, df: DataFrame) -> DataFrame:
        """
        Extracts a timezone using latitude and longitude columns,
        and outputs the input DataFrame with a TimeZone column.
        """
        columns = df.columns

        if self.latitude_col not in columns:
            raise ColumnDoesNotExistException(
                "The specified latitude column is not in the DataFrame"
            )

        if self.longitude_col not in columns:
            raise ColumnDoesNotExistException(
                "The specified longitude column is not in the DataFrame"
            )

        timezone_extractor = F.udf(
            lambda latitude, longitude: (
                None
                if latitude is None or longitude is None
                else TimezoneFinder().timezone_at(lat=latitude, lng=longitude)
            )
        )

        df = df.withColumn(
            self.column_output_name,
            timezone_extractor(F.col(self.latitude_col), F.col(self.longitude_col)),
        )

        return df

    def process_many(self, datasets: dataset_group) -> DataFrame:
        raise NotImplementedError()
