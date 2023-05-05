import uuid as _uuid
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.streaming import DataStreamWriter

from spetlr.etl import Loader
from spetlr.exceptions import (
    AmbiguousLoaderInput,
    MissingEitherStreamLoaderOrHandle,
    NeedTriggerTimeWhenProcessingType,
    NotAValidStreamTriggerType,
    UnknownStreamOutputMode,
)
from spetlr.functions import init_dbutils
from spetlr.spark import Spark
from spetlr.tables import TableHandle
from spetlr.utils.FileExists import file_exists


class StreamLoader(Loader):
    def __init__(
        self,
        *,
        format: str,
        options_dict: dict,
        checkpoint_path: str,
        mode: str = "overwrite",
        trigger_type: str = "availablenow",
        handle: TableHandle = None,
        loader: Loader = None,
        trigger_time_seconds: int = None,
        outputmode: str = "update",
        query_name: str = None,
        await_termination: bool = False,
        upsert_join_cols: List[str] = None,
    ):
        """
        checkpoint_path: The location of the checkpoints, <table_name>/_checkpoints
            The Delta Lake VACUUM function removes all files not managed by Delta Lake
            but skips any directories that begin with _. You can safely store
            checkpoints alongside other data and metadata for a Delta table
            using a directory structure such as <table_name>/_checkpoints
            See: https://docs.databricks.com/structured-streaming/delta-lake.html
        location: the location of the delta table (Optional)
        data_format: the data format of the files that are read (Default delta)
        trigger_type: the trigger type of the stream.
            See: https://docs.databricks.com/structured-streaming/triggers.html
        trigger_time: if the trigger has is "processingtime",
            it should have a trigger time associated
        """

        super().__init__()
        self._mode = mode
        self._handle = handle

        self._format = format
        self._options_dict = options_dict
        self._outputmode = outputmode
        self._trigger_type = trigger_type
        self._trigger_time_seconds = trigger_time_seconds
        self._query_name = query_name or str(_uuid.uuid4().hex)
        self._loader = loader
        self._checkpoint_path = checkpoint_path  # or self._handle.get_checkpoint_path()
        self._await_termination = await_termination
        self._join_cols = upsert_join_cols

        if self._handle is None and self._loader is None:
            raise MissingEitherStreamLoaderOrHandle

        if self._handle is not None and self._loader is not None:
            raise AmbiguousLoaderInput

        assert (
            Spark.version() >= Spark.DATABRICKS_RUNTIME_10_4
        ), f"DeltaStreamHandle not available for Spark version {Spark.version()}"

    def save(self, df: DataFrame) -> None:
        # Set checkpoint path always
        self._options_dict = (
            self._options_dict if self._options_dict is not None else {}
        )
        self._options_dict["checkpointLocation"] = self._checkpoint_path

        # "continuous" is not available when using foreachBatch()
        valid_trigger_types = {"availablenow", "once", "processingtime"}

        if self._trigger_type not in valid_trigger_types:
            raise NotAValidStreamTriggerType()

        if (self._trigger_type == "processingtime") and (
            self._trigger_time_seconds is None
        ):
            raise NeedTriggerTimeWhenProcessingType

        if self._outputmode not in {"complete", "append", "update"}:
            raise UnknownStreamOutputMode

        df_stream = (
            df.writeStream.format(self._format)
            .options(**self._options_dict)
            .outputMode(self._outputmode)
            .queryName(self._query_name)
        )

        df_stream = self._add_trigger_type(df_stream)

        df_stream.foreachBatch(self._foreachbatch)

        query = df_stream.start()

        if self._await_termination:
            query.awaitTermination()

    def _foreachbatch(
        self,
        df: DataFrame,
        batchId: int = None,
    ):
        if self._loader:
            self._loader.save(df)
        elif self._mode == "append":
            self._handle.append(df)
        elif self._mode == "overwrite":
            self._handle.overwrite(df)
        elif self._mode == "upsert":
            self._handle.upsert(df, self._join_cols)
        else:
            raise ValueError()

    def _add_trigger_type(self, writer: DataStreamWriter):
        if self._trigger_type == "availablenow":
            return writer.trigger(availableNow=True)
        elif self._trigger_type == "once":
            return writer.trigger(once=True)
        elif self._trigger_type == "processingtime":
            return writer.trigger(
                processingTime=f"{self._trigger_time_seconds} seconds",
            )
        # Continuous is not availble when using foreachBatch()
        # https://docs.databricks.com/structured-streaming/foreach.html#apply-additional-dataframe-operations

        # elif self._trigger_type == "continuous":
        #    return writer.trigger(continuous=f"{self._trigger_time_seconds} seconds")
        else:
            raise ValueError("Unknown trigger type.")

    def _validate_checkpoint(self):
        if "/_" not in self._checkpoint_path:
            print(
                "RECOMMENDATION: You can safely store checkpoints alongside "
                "other data and metadata for a Delta table using a directory "
                "structure such as <table_name>/_checkpoints"
            )

    def remove_checkpoint(self):
        if not file_exists(self._checkpoint_path):
            init_dbutils().fs.mkdirs(self._checkpoint_path)
