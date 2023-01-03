import pyspark.sql.functions as f
from pyspark.sql import DataFrame

from atc.eh import EventHubCapture
from atc.etl import Extractor
from atc.etl.extractors.simple_extractor import Readable


class IncrementalExtractor(Extractor):
    """This extractor will extract from any object that has a .read() method.
    Furthermore, it will use a target table for enabling incremental extraction.

    NB: It is not recommended to use this on Eventhub data. Use EventHubCaptureExtractor instead.

    """

    def __init__(
        self,
        handleSource: Readable,
        handleTarget: Readable,
        timeCol: str,
        dataset_key: str,
    ):
        super().__init__(dataset_key=dataset_key)
        self.handle_source = handleSource
        self.handle_target = handleTarget
        self._timecol = timeCol

    def read(self) -> DataFrame:

        if isinstance(self.handle_source, EventHubCapture):
            print(
                "It is recommended to use EventHubCaptureExtractor for extracting eventhub data."
                "EventHubCaptureExtractor is optimized for reading avro data."
            )

        df = self.handle_source.read()
        df_target = self.handle_target.read()

        # For incremental load, get the latest (max) record from target table
        maxtime = df_target.groupBy().agg(f.max(self._timecol)).collect()[0][0]

        # If the target is empty, this will be None
        if maxtime:
            # If it is > or >= should be investigated
            # due to e.g. capture file readings....
            df = df.where(f.col(self._timecol) > f.lit(maxtime))

        return df
