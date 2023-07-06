from pyspark.sql import DataFrame

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
        schema_location: str,
        data_format: str,
        options: dict = None
    ):
        """
        file_location: the path to the file location of the delta table
        schema_location: The location of the cloudfile schema.
            Databricks documentation uses the expected writeStream checkpoint_path
            as schema_location, see documentation at
            https://docs.databricks.com/getting-started/etl-quick-start.html#auto-loader

        data_format: The expected data format extracted by the Autoloader.
            Examples: JSON, CSV, PARQUET, AVRO, ORC, TEXT, and BINARYFILE.
            See: https://docs.databricks.com/ingestion/auto-loader/index.html
        """

        if Spark.version() < Spark.DATABRICKS_RUNTIME_10_4:
            raise SparkVersionNotSupportedForSpetlrStreaming()

        self._location = file_location
        self._data_format = data_format

        self._schema_location = schema_location
        self._options = options

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
