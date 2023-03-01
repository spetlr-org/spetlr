import pyspark.sql.functions as f
from atc_tools.time import dt_utc
from pyspark.sql import DataFrame

from atc.delta import DeltaHandle
from atc.etl import Transformer


class EhToDeltaBronzeTransformer(Transformer):
    def __init__(self, target_dh: DeltaHandle):
        super().__init__()
        self.target_dh = target_dh
        self._eh_cols = [
            "BodyId",
            "Body",
            "EnqueuedTimestamp",
            "StreamingTime",
            "pdate",
        ]

    def process(self, df: DataFrame) -> DataFrame:
        target_df = self.target_dh.read()

        assert set(self._eh_cols).issubset(target_df.columns)

        # Generate id for the eventhub rows using hashed body
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
            f.col("BodyId"),
            f.col("Body").cast("string"),
            f.col("EnqueuedTimestamp"),
            f.col("StreamingTime"),
            f.col("pdate"),
        )

        # Ensure that cols are selected correctly
        df = df.select(*self._eh_cols)

        return df
