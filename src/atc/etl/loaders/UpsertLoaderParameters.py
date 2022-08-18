from typing import List

from atc.delta import DeltaHandle


class UpsertLoaderParameters:
    def __init__(
        self,
        incremental_load: bool,
        target_id: str,
        join_cols: List[str],
        target_dh: DeltaHandle = None,
    ):

        self.incremental_load = incremental_load

        # The id of the target dataframe to perform incremental load on
        self.target_id = target_id
        self.target_dh = target_dh or DeltaHandle.from_tc(target_id)

        # The key columns to be used in joining/merging the existing and incoming data
        self.join_cols = join_cols
