import datetime

import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.tables import TableHandle


class EhToDeltaBronzeTransformer(Transformer):
    """
    This class transforms eventhub data into the following schema:

    | EventhubRowId | BodyId | Body | EnqueuedTimestamp | StreamingTime |->
    |---------------|--------|------|-------------------|---------------|->
    |...            |...     |...   |...                |...            |->

    | SequenceNumber | Offset | SystemProperties | Properties| pdate |
    |----------------|--------|------------------|-----------|-------|
    |...             |...     |...               |...        |...    |

    Parameters:
    target_dh: DeltaHandle for the target delta table (bronze)
    df: A dataframe containing raw eventhub data

    Returns:
    A dataframe with the above mentioned schema


    This class uses a combination of Body and EnqueuedTimestamp
    for creating hashed values as Ids.

    NB: The hashed value could potentially become NULL
    """

    def __init__(self, target_dh: TableHandle):
        super().__init__()
        self.target_dh = target_dh
        self._eh_cols = [
            "EventhubRowId",
            "BodyId",
            "Body",
            "EnqueuedTimestamp",
            "StreamingTime",
            "SequenceNumber",
            "Offset",
            "SystemProperties",
            "Properties",
            "pdate",
        ]

    def process(self, df: DataFrame) -> DataFrame:

        target_df = self.target_dh.read()

        assert set(self._eh_cols).issubset(target_df.columns)

        # Generate Unique id for the eventhub rows
        # xxhash64 accepts multiple columns and returns a BIGINT
        # NB: Can be NULL
        df = df.withColumn(
            "EventhubRowId",
            f.xxhash64(
                f.col("Body").cast("string"),
                f.col("EnqueuedTimestamp").cast("string"),
            ).cast("long"),
        )

        # Generate id for the eventhub rows using hashed body
        # Can be used for identify rows with same body
        # NB: Can be NULL
        df = df.withColumn(
            "BodyId", f.xxhash64(f.col("Body").cast("string")).cast("long")
        )

        # Add streaming time
        streaming_time = datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        )
        df = df.withColumn("StreamingTime", f.lit(streaming_time))

        # Cast body to string
        df = df.select(
            f.col("EventhubRowId").cast("long").alias("EventhubRowId"),
            f.col("BodyId").cast("long").alias("BodyId"),
            f.col("Body").cast("string").alias("Body"),
            f.col("EnqueuedTimestamp").cast("timestamp").alias("EnqueuedTimestamp"),
            f.col("StreamingTime").cast("timestamp").alias("StreamingTime"),
            f.col("SequenceNumber").cast("long").alias("SequenceNumber"),
            f.col("Offset").cast("string").alias("Offset"),
            f.col("SystemProperties").cast("string").alias("SystemProperties"),
            f.col("Properties").cast("string").alias("Properties"),
            f.col("pdate").cast("timestamp").alias("pdate"),
        )

        # Ensure that cols are selected correctly
        df = df.select(*self._eh_cols)

        return df
