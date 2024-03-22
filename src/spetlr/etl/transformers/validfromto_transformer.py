from datetime import datetime, timezone
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Window

from spetlr.etl import Transformer
from spetlr.utils import DropOldestDuplicates


class ValidFromToTransformer(Transformer):
    """
    The class introduces SCD2 columns:
        ValidFrom
        ValidTo
        IsCurrent
    NB: Be aware, if incremental extraction is used, the logic does not work
    """

    def __init__(
        self,
        time_col: str,
        wnd_cols: List[str],
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self._time_col = time_col
        self._wnd_cols = wnd_cols

    def process(self, df: DataFrame) -> DataFrame:
        _cols = df.columns
        df = df.withColumn("ValidFrom", f.col(self._time_col))

        # Drop duplicates based on the window cols and time col
        # If there is duplicates,
        # the ValidFromTransformer could introduce multiple is_current
        df = DropOldestDuplicates(
            df=df,
            cols=self._wnd_cols + [self._time_col],
            orderByColumn=self._time_col,
        )

        # The largest year number allowed in python is 9999
        # but using this value can cause some pyspark errors
        # When .collect() the data, the pyspark/sql/types.py
        # find it as an "Invalid argument"

        # Therefore, this class uses the max time from pandas
        # as it seems to align with pyspark
        # https://pandas.pydata.org/docs/reference/api/pandas.Timestamp.max.html

        max_time = datetime(2262, 4, 11, tzinfo=timezone.utc)

        return (
            df.withColumn(
                "NextValidFrom",
                f.lead("ValidFrom").over(
                    Window.partitionBy(self._wnd_cols).orderBy("ValidFrom")
                ),
            )
            .withColumn("ValidTo", f.coalesce(f.col("NextValidFrom"), f.lit(max_time)))
            .withColumn("IsCurrent", f.col("NextValidFrom").isNull())
            .drop("NextValidFrom")
            .select(*_cols, "ValidFrom", "ValidTo", "IsCurrent")
        )
