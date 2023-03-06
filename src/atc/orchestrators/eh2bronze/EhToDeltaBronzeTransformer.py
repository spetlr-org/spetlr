import pyspark.sql.functions as f
from atc_tools.time import dt_utc
from pyspark.sql import DataFrame

from atc.etl import Transformer
from atc.tables import TableHandle


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
        df = df.withColumn(
            "EventhubRowId",
            f.conv(
                f.concat_ws(
                    "",
                    f.lit("0"),
                    f.substring(
                        f.concat_ws(
                            "",
                            f.sha2(f.col("Body").cast("string"), 256),
                            f.sha2(f.col("EnqueuedTimestamp").cast("string"), 256),
                        ),
                        -15,
                        15,
                    ),
                ),
                16,
                10,
            ).cast("long"),
        )

        # Generate id for the eventhub rows using hashed body
        # Can be used for identify rows with same body
        df = df.withColumn(
            "BodyId",
            f.conv(
                f.concat_ws(
                    "",
                    f.lit("0"),
                    f.substring(f.sha2(f.col("Body").cast("string"), 256), -15, 15),
                ),
                16,
                10,
            ).cast("long"),
        )

        # Add streaming time
        streaming_time = dt_utc().replace(microsecond=0)
        df = df.withColumn("StreamingTime", f.lit(streaming_time))

        # Cast body to string
        df = df.select(
            f.col("EventhubRowId").cast("long"),
            f.col("BodyId").cast("long"),
            f.col("Body").cast("string"),
            f.col("EnqueuedTimestamp").cast("timestamp"),
            f.col("StreamingTime").cast("timestamp"),
            f.col("SequenceNumber").cast("long"),
            f.col("Offset").cast("string"),
            f.col("SystemProperties").cast("string"),
            f.col("Properties").cast("string"),
            f.col("pdate").cast("timestamp"),
        )

        # Ensure that cols are selected correctly
        df = df.select(*self._eh_cols)

        return df
