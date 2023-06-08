import uuid as _uuid
import warnings

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from spetlr.etl import Loader
from spetlr.exceptions import (
    NeedTriggerTimeWhenProcessingType,
    NotAValidStreamTriggerType,
    UnknownStreamOutputMode,
)
from spetlr.spark import Spark


class StreamLoader(Loader):
    def __init__(
        self,
        *,
        loader: Loader,
        checkpoint_path: str = None,
        options_dict: dict = None,
        trigger_type: str = "availablenow",
        trigger_time_seconds: int = None,
        outputmode: str = "update",
        query_name: str = None,
        await_termination: bool = False,
    ):
        """
        checkpoint_path: The location of the checkpoints, <table_name>/_checkpoints
            The Delta Lake VACUUM function removes all files not managed by Delta Lake
            but skips any directories that begin with _. You can safely store
            checkpoints alongside other data and metadata for a Delta table
            using a directory structure such as <table_name>/_checkpoints
            See: https://docs.databricks.com/structured-streaming/delta-lake.html
        location: the location of the delta table (Optional)
        trigger_type: the trigger type of the stream.
            See: https://docs.databricks.com/structured-streaming/triggers.html
        trigger_time: if the trigger has is "processingtime",
            it should have a trigger time associated
        """

        super().__init__()
        self._loader = loader
        self._options_dict = options_dict
        self._outputmode = outputmode
        self._trigger_type = trigger_type
        self._trigger_time_seconds = trigger_time_seconds
        self._query_name = query_name or str(
            _uuid.uuid4().hex
        )  # Consider if this is smart?
        self._checkpoint_path = checkpoint_path
        self._await_termination = await_termination
        self._validate_checkpoint()

        assert (
            Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4
        ), f"DeltaStreamHandle not available for Spark version {Spark.version()}"

        # Set checkpoint path always
        self._options_dict = self._options_dict or {}

        assert "checkpointLocation" not in self._options_dict

        self._options_dict["checkpointLocation"] = self._checkpoint_path

        # "continuous" is not available when using foreachBatch()
        # https://docs.databricks.com/structured-streaming/foreach.html#apply-additional-dataframe-operations

        if self._trigger_type not in {"availablenow", "once", "processingtime"}:
            raise NotAValidStreamTriggerType()

        if (self._trigger_type == "processingtime") and (
            self._trigger_time_seconds is None
        ):
            raise NeedTriggerTimeWhenProcessingType()

        if self._outputmode not in {"complete", "append", "update"}:
            raise UnknownStreamOutputMode()

    def save(self, df: DataFrame) -> None:
        df_stream = (
            df.writeStream.options(**self._options_dict)
            .outputMode(self._outputmode)
            .queryName(self._query_name)
        )

        df_stream = self._add_trigger_type(df_stream)

        df_stream.foreachBatch(self._foreachbatch)

        query = df_stream.start()

        if self._await_termination:
            query.awaitTermination()

    def _foreachbatch(self, df: DataFrame, _: int = None):
        self._loader.save(df)

    def _add_trigger_type(self, writer: DataStreamWriter):
        if self._trigger_type == "availablenow":
            return writer.trigger(availableNow=True)
        elif self._trigger_type == "once":
            return writer.trigger(once=True)
        elif self._trigger_type == "processingtime":
            return writer.trigger(
                processingTime=f"{self._trigger_time_seconds} seconds",
            )
        else:
            raise ValueError("Unknown trigger type.")

    def _validate_checkpoint(self):
        if "/_" not in self._checkpoint_path:
            warnings.warn(
                "RECOMMENDATION: You can safely store checkpoints alongside "
                "other data and metadata for a Delta table using a directory "
                "structure such as <table_name>/_checkpoints"
            )
