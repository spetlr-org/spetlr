import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from atc.eh import EventHubCapture
from atc.etl import Extractor
from atc.etl.extractors.simple_extractor import Readable


class IncrementalExtractor(Extractor):
    """This extractor will extract from any object that has a .read() method.
    Furthermore, it will use a target table for enabling incremental extraction.

    NB: It is not recommended to use this on Eventhub data.
        Use EventHubCaptureExtractor instead.

    """

    def __init__(
        self,
        handle_source: Readable,
        handle_target: Readable,
        time_col_source: str,
        time_col_target: str,
        dataset_key: str = None,
    ):
        super().__init__(dataset_key=dataset_key)
        self.handle_source = handle_source
        self.handle_target = handle_target
        self._timecol_source = time_col_source
        self._timecol_target = time_col_target

    def read(self) -> DataFrame:
        if isinstance(self.handle_source, EventHubCapture):
            print(
                "It is recommended to use EventHubCaptureExtractor "
                "for extracting eventhub data."
                "EventHubCaptureExtractor is optimized for reading avro data."
            )

        df = self.handle_source.read()
        df_target = self.handle_target.read()

        # For incremental load, get the latest record from target table
        # In other words, get the maximum of the timestamp column
        target_max_time = (
            df_target.groupBy().agg(f.max(self._timecol_target)).collect()[0][0]
        )

        # If the target table is empty, target_max_time will be None
        # Only filter the input dataframe if the table is non-empty
        # table non-empty = target_max_time not None
        if target_max_time:
            df = df.where(f.col(self._timecol_source) > f.lit(target_max_time))

        return df
