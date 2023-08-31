from typing import List

from spetlr.delta import DeltaHandle
from spetlr.etl import EtlBase, Orchestrator
from spetlr.etl.extractors import IncrementalExtractor, SimpleExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.exceptions import MissingUpsertJoinColumns
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)


class EhToDeltaSilverOrchestrator(Orchestrator):
    """
    This class has been designed to carry out the ETL task
    of unpacking and transforming bronze eventhub data to the silver layer.

    Parameters:

    dh_source: DeltaHandle for the source delta table (bronze)
    dh_target: DeltaHandle for the target delta table (silver)
    upsert_join_cols: The columns used for upserting.
    mode: The mode of data load (upsert, append, overwrite).

    Returns:
    Processed datasets of the super Orchestrator class
    """

    def __init__(
        self,
        dh_source: DeltaHandle,
        dh_target: DeltaHandle,
        upsert_join_cols: List[str] = None,
        mode: str = "upsert",
    ):
        super().__init__()
        self.dh_source = dh_source
        self.dh_target = dh_target
        self.mode = mode
        self.upsert_join_cols = upsert_join_cols

        # step 1
        # Extracts the data from the bronze layer
        if mode == "upsert" or mode == "append":
            if mode == "upsert":
                if upsert_join_cols is None:
                    raise MissingUpsertJoinColumns
            self.extract_from(
                IncrementalExtractor(
                    handle_source=self.dh_source,
                    handle_target=self.dh_target,
                    time_col_source="EnqueuedTimestamp",
                    time_col_target="EnqueuedTimestamp",
                )
            )

        else:
            self.extract_from(SimpleExtractor(dh_source, "EhDeltaBronze"))

        # step 2,
        #  - use the target schema to select what to copy from capture files
        #  - anything that is not in the source df is used to unpack the body json
        self.transform_with(EhJsonToDeltaTransformer(target_dh=dh_target))

        # the method filter_with can be used to insert any number of transformers here

        self._loader = SimpleLoader(
            dh_target, join_cols=self.upsert_join_cols, mode=self.mode
        )

        self.load_into(self._loader)

    @classmethod
    def from_tc(cls, dh_source_id: str, dh_target_id: str):
        return cls(
            dh_source=DeltaHandle.from_tc(dh_source_id),
            dh_target=DeltaHandle.from_tc(dh_target_id),
        )

    def filter_with(self, etl: EtlBase):
        """Additional filters to execute before loading."""
        loader = self.steps.pop()

        # the following will fail if additional steps have been added.
        assert self._loader is loader, "unexpected change in etl steps"

        self.transform_with(etl)
        self.load_into(loader)
