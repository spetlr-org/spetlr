from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd
from dateutil.parser import parse
from pandas.core.generic import NDFrameT
from pyspark.sql import DataFrame
from pytz import timezone, utc

from spetlr.power_bi.PowerBiException import PowerBiException
from spetlr.spark import Spark


class SparkPandasDataFrame:
    # converts data types from Spark to Pandas and vice versa
    SPARK_TO_PANDAS_TYPES = {
        "long": "Int64",
        "int": "Int64",
        "timestamp": "datetime64[ns]",
        "boolean": "bool",
        "string": "object",
        "default": "object",
    }

    def __init__(
        self,
        json: Union[Dict, List],
        schema: List[Tuple[Union[str, Callable[[pd.DataFrame], NDFrameT]], str, str]],
        *,
        indexing_columns: Optional[Union[int, List[int], str, List[str]]] = None,
        sorting_columns: Optional[Union[int, List[int], str, List[str]]] = None,
        ascending: bool = True,
        local_timezone_name: str = None,
    ):
        """
        Provides a class for loading a JSON into Pandas, and for converting
        between Pandas and Spark.

        :param dict json: Data source, which is a dictionary representing a JSON.
        :param dict[tuple[str/callable, str, str]: schema definition where the first
            value in the tuple is the original column name in the JSON, the second
            is the desired column name in the output, and the third is the Spark type.
            The first value can also be a lambda expression taking a Pandas
            data frame as input and calculating the column value from
            other available columns, e.g. lambda df: df["endTime"] - df["startTime"]
        :param int/str indexing_columns: indexes (within the schema) or names
            of the indexing columns.
        :param int/str sorting_columns: indexes (within the schema) or names
            of the sorting columns.
        :param str local_timezone_name: The timezone to use when parsing
            timestamp columns. The default timezone is UTC.
            If the timezone is UTC, all timestamp columns will have a suffix "Utc".
            Otherwise they will have a suffix "Local".
        """

        self.local_timezone_name = local_timezone_name or "UTC"
        self.is_utc = self.local_timezone_name.upper() == "UTC"
        self.time_column_suffix = "Utc" if self.is_utc else "Local"
        self.schema = schema

        for i, column in enumerate(schema):
            if column[2] == "timestamp":
                schema[i] = (column[0], column[1] + self.time_column_suffix, column[2])
        df = pd.DataFrame(
            json,
            columns=[column[0] for column in schema if isinstance(column[0], str)],
        )
        if df.empty:
            self.df = pd.DataFrame()
        else:
            df = df.replace(np.nan, None)
            if indexing_columns is not None:
                df.set_index(self._get_columns(indexing_columns, False))
            for column in schema:
                if isinstance(column[0], str):
                    if column[2] == "timestamp":
                        df[column[0]] = df[column[0]].apply(
                            lambda text: self._parse_time(text)
                        )
                    else:
                        df[column[0]] = df[column[0]].astype(
                            self.SPARK_TO_PANDAS_TYPES.get(
                                column[2], self.SPARK_TO_PANDAS_TYPES["default"]
                            )
                        )
            for i, column in enumerate(schema):
                if callable(column[0]):
                    df.insert(
                        i,
                        column[1],
                        column[0](df).astype(
                            self.SPARK_TO_PANDAS_TYPES.get(
                                column[2], self.SPARK_TO_PANDAS_TYPES["default"]
                            )
                        ),
                    )
            for column in schema:
                if column[2] == "timestamp" and isinstance(column[0], str):
                    df[column[0]] = df[column[0]].apply(
                        lambda text: self._localize_time(text, self.local_timezone_name)
                    )
            df = df.rename(
                columns=dict(
                    (column[0], column[1])
                    for column in schema
                    if isinstance(column[0], str)
                ),
            )
            if sorting_columns is not None:
                df = df.sort_values(
                    self._get_columns(sorting_columns, True), ascending=ascending
                )
            self.df = df

    def _get_columns(
        self, identifiers: Union[int, List[int], str, List[str]], target: bool
    ) -> Union[str, List[str]]:
        """
        Returns the name of the specified column in the schema.

        :param int/str identifiers: the column(s) to look for, which is either
            the column index(es), or column source name(s), or column target name(s).
        :param bool target: return the target column name if target==True,
            or source column name otherwise.
        :return: the requested column
        :rtype: str
        :raises PowerBiException: if the column cannot be found
        """

        result = []
        if isinstance(identifiers, int) or isinstance(identifiers, str):
            identifiers = [identifiers]
        if isinstance(identifiers, list):
            for identifier in identifiers:
                if isinstance(identifier, int) and identifier < len(self.schema):
                    result.append(self.schema[identifier][int(target)])
                elif isinstance(identifier, str):
                    result.append(
                        next(
                            (
                                column[int(target)]
                                for column in self.schema
                                if identifier == column[0] or identifier == column[1]
                            ),
                            None,
                        )
                    )
        if not result or (
            isinstance(identifiers, list) and len(result) != len(identifiers)
        ):
            raise PowerBiException(f"{identifiers} column(s) not found in the schema")
        return result if len(result) > 1 else result[0]

    @staticmethod
    def _parse_time(text: str) -> Optional[datetime]:
        """
        Parses an ISO 8601 time text and converts it to datetime.
        The result is a timezone-unaware UTC datetime regardless of input.

        :param str text: an ISO 8601 time text or None
        :return: timezone-unaware UTC datetime or None
        :rtype: datetime
        """

        if text is None or text != text:
            return None
        time = parse(text)
        if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
            time = utc.localize(time)
        else:
            time = time.astimezone(utc)
        return time.replace(microsecond=0, tzinfo=None)

    @staticmethod
    def _localize_time(
        time: Optional[datetime], local_timezone_name: str
    ) -> Optional[datetime]:
        """
        Converts a timezone-unaware UTC datetime to a timezone-unaware local datetime.

        :param datetime time: timezone-unaware UTC datetime or None
        :return: timezone-unaware local datetime or None
        :rtype: datetime
        """

        if time is None or time != time:
            return None
        if time.tzinfo is None or time.tzinfo.utcoffset(time) is None:
            time = utc.localize(time)
        time = time.astimezone(timezone(local_timezone_name))
        return time.replace(tzinfo=None)

    def get_pandas_df(self) -> Optional[pd.DataFrame]:
        """
        Returns the data frame as a Pandas data frame.

        :return: Pandas data frame
        :rtype: Pandas data frame
        """

        return self.df

    def get_spark_df(self) -> Optional[DataFrame]:
        """
        Returns the data frame as a Spark data frame.

        :return: Spark data frame
        :rtype: Spark data frame
        """

        df = self.df
        if df is None:
            return None
        spark_schema = ", ".join(f"{column[1]} {column[2]}" for column in self.schema)
        if df.empty:
            return Spark.get().createDataFrame(df, spark_schema)
        else:
            return Spark.get().createDataFrame(df)

    def show(
        self,
        message: str,
        when_empty: str,
        *,
        filter_columns: Optional[List[Tuple[str, str]]] = None,
    ) -> None:
        """
        Displays the Pandas data frame.

        :param str message: The message to show when the data frame is not empty.
        :param str when_empty: The message to show when the data frame is empty.
           Note: Pandas fails when showing empty data frames.
        :param list[tuple[str, str]] filter_columns: a subset of columns to show
            and their new names
        :rtype: None
        """

        if self.df is None or self.df.empty:
            print(when_empty)
        else:
            print(message)
            df = self.df
            if filter_columns:
                df = df.rename(columns=dict(filter_columns))
                df = df[[column[1] for column in filter_columns]]
            df.display()

    def get_local_time_str(self, time: datetime) -> Optional[str]:
        """
        Returns the specified time as a local timestamp converted to text.

        :param datetime time: the time to convert from
        :return: text of a timezone-aware local datetime or None
        :rtype: str
        """

        time_description_suffix = " (UTC)" if self.is_utc else " (local time)"
        if time is not None:
            return time.strftime("%Y-%m-%d %H:%M") + time_description_suffix
        return None

    def get_utc_time(self, time: datetime) -> Optional[datetime]:
        """
        Returns the specified time as an UTC timestamp.

        :param datetime time: the time to convert from
        :return: a timezone-aware UTC datetime or None
        :rtype: datetime
        """

        zone = timezone(self.local_timezone_name)
        if time is not None:
            if self.is_utc:
                time = utc.localize(time)
            else:
                time = zone.localize(time).astimezone(utc)
        return time

    def append(
        self,
        source,
        prefix_columns: List[Tuple[str, str, str]],
        suffix_columns: List[Tuple[str, str, str]],
    ):
        """
        Appends another instance of this class to this data frame, which will result
        in a union of two Pandas data frames. Additionally, new columns should
        be added to this instance, which are already present in the other instance.
        This will allow to make a distinction between both data frames.

        :param SparkPandasDataFrame source: another object of this class
            that needs to be appended to this data frame
        :param list[tuple[str, str]] prefix_columns: new columns with values
            to add at the beginning
        :param list[tuple[str, str]] suffix_columns: new columns with values
            to add at the end
        :rtype: SparkPandasDataFrame
        """
        for column in reversed(prefix_columns):
            self.schema.insert(0, (column[0], column[0], column[2]))
            if not self.df.empty:
                self.df.insert(0, column[0], column[1])
        for column in suffix_columns:
            self.schema.append((column[0], column[0], column[2]))
            if not self.df.empty:
                self.df[column[0]] = column[1]
        if source is not None and not source.get_pandas_df().empty:
            self.df = pd.concat([source.get_pandas_df(), self.df])
        if not self.df.empty:
            self.df = self.df.reset_index(drop=True)
        return self
