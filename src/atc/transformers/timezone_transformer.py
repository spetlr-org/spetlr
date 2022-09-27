import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from timezonefinder import TimezoneFinder

from atc.etl.extended_transformer import ExtendedTransformer


class TimeZoneTransformer(ExtendedTransformer):
    """
    This transformer extracts a timezone using longitude and latitude.

    Attributes:
    ----------
        longitude_col : float
            name of column with longitude values
        latitude_col : float
            name of column with latitude values
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
        longitude_col: float,
        latitude_col: float,
        dataset_input_key: str,
        dataset_output_key: str,
        column_output_name: str = "TimeZone",
    ):
        super().__init__(
            dataset_input_key=dataset_input_key, dataset_output_key=dataset_output_key
        )
        self.longitude_col = longitude_col
        self.latitude_col = latitude_col
        self.column_output_name = column_output_name

    def process(self, df: DataFrame) -> DataFrame:
        """
        Extracts a timezone using latitude and longitude columns,
        and outputs the input DataFrame with a TimeZone column.
        """
        timezone_extractor = F.udf(
            lambda longitude, latitude: None
            if longitude is None or latitude is None
            else TimezoneFinder().timezone_at(lng=longitude, lat=latitude)
        )

        df = df.withColumn(
            self.column_output_name,
            timezone_extractor(F.col(self.longitude_col), F.col(self.latitude_col)),
        )

        return df
