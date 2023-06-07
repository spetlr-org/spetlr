from pyspark.sql import DataFrame

from spetlr.configurator.configurator import Configurator
from spetlr.delta.delta_handle import DeltaHandleInvalidFormat
from spetlr.spark import Spark
from spetlr.tables import TableHandle


class AutoloaderHandle(TableHandle):
    def __init__(
        self,
        *,
        location: str,
        schema_location: str,
        data_format: str,
    ):
        """
        location: the location of the delta table
        checkpoint_path: The location of the checkpoints, <table_name>/_checkpoints
            The Delta Lake VACUUM function removes all files not managed by Delta Lake
            but skips any directories that begin with _. You can safely store
            checkpoints alongside other data and metadata for a Delta table
            using a directory structure such as <table_name>/_checkpoints
            See: https://docs.databricks.com/structured-streaming/delta-lake.html
        data_format: the data format of the files that are read
        """

        assert (
            Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4
        ), f"AutoloaderStreamHandle not available for Spark version {Spark.version()}"

        self._location = location
        self._data_format = data_format

        self._schema_location = schema_location

        self._validate()

    @classmethod
    def from_tc(cls, id: str) -> "AutoloaderHandle":
        tc = Configurator()
        return cls(
            location=tc.table_property(id, "path", ""),
            data_format=tc.table_property(id, "format", ""),
            schema_location=tc.table_property(id, "schema_location", ""),
        )

    def _validate(self):
        """Validates that the name is either db.table or just table."""
        if self._data_format == "delta":
            raise DeltaHandleInvalidFormat("Use DeltaHandle.read_stream() for delta.")

    def read_stream(self) -> DataFrame:
        reader = (
            Spark.get()
            .readStream.format("cloudFiles")
            .option("cloudFiles.format", self._data_format)
            .option("cloudFiles.schemaLocation", self._schema_location)
            .load(self._location)
        )

        return reader
