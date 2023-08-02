from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spetlr.configurator.configurator import Configurator
from spetlr.delta.delta_handle import DeltaHandleInvalidFormat
from spetlr.exceptions import SparkVersionNotSupportedForSpetlrStreaming
from spetlr.spark import Spark
from spetlr.tables import TableHandle


class FileHandle(TableHandle):
    def __init__(
        self,
        *,
        file_location: str,
        data_format: str,
        schema: StructType = None,
        schema_location: str = None,
        options: Dict[str, Any] = None,
    ):
        """
        A handle for reading files.

        Arguments:
            file_location (str): The path where the files are stored. When using
                'read_stream'. see documentation at:
                https://docs.databricks.com/ingestion/auto-loader/index.html
            data_format (str): The expected data format to be handled. Examples: JSON,
                CSV, PARQUET, AVRO, ORC, TEXT, and BINARYFILE. When using 'read_stream'
                Auto Loader processing is used, see documentation at:
                https://docs.databricks.com/getting-started/etl-quick-start.html#auto-loader
            schema (StructType, optional): The schema to apply when reading. If not
                supplied the schema is inferred.
            schema_location (str, optional): When 'read_stream' is used, the schema
                location for storing the read schema. See Auto Loader documentation
                provided above. It is recommended to use the checkpoint path.


        Methods:
            read() -> DataFrame: Reads the file(s) at the file location using the data
                format provided.
            read_stream() -> DataFrame: Reads the file(s) using Auto Loader processing.
                See the documentation at:
                https://docs.databricks.com/en/ingestion/auto-loader/index.html

        Note that although the handle does not currently support writing in may be
        implemented in the future.
        """

        if Spark.version() < Spark.DATABRICKS_RUNTIME_10_4:
            raise SparkVersionNotSupportedForSpetlrStreaming()

        self._location = file_location
        self._data_format = data_format

        self._schema_location = schema_location
        self._options = options
        self._schema = schema

        self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "FileHandle":
        tc = Configurator()
        return cls(
            file_location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", ""),
            schema_location=tc.table_property(id, "schema_location", ""),
        )

    def _validate(self):
        """
        Validates the dataformat is not delta,
        since delta stream should use the DeltaHandle instead.
        """
        if self._data_format == "delta":
            raise DeltaHandleInvalidFormat("Use DeltaHandle.read_stream() for delta.")

    def read_stream(self) -> DataFrame:
        reader = (
            Spark.get()
            .readStream.format("cloudFiles")
            .option("cloudFiles.format", self._data_format)
            .option("cloudFiles.schemaLocation", self._schema_location)
        )

        if self._options is not None:
            reader = reader.options(**self._options)

        reader = reader.load(self._location)

        return reader

    def read(self) -> DataFrame:
        reader = Spark.get().read

        reader = reader.format(self._data_format)

        if self._schema:
            reader = reader.schema(self._schema)

        if self._options:
            reader = reader.options(**self._options)

        return reader.load(self._location)
