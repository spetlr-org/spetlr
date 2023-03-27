from spetlr.delta import DeltaHandle
from spetlr.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from spetlr.etl import EtlBase, Orchestrator
from spetlr.etl.loaders import SimpleLoader
from spetlr.orchestrators.eh2bronze.EhToDeltaBronzeTransformer import (
    EhToDeltaBronzeTransformer,
)
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import (
    EhJsonToDeltaExtractor,
)


class EhToDeltaBronzeOrchestrator(Orchestrator):
    """

    This class has been designed to carry out the ETL task
    of ingest eventhub data to a bronze layer.

    Parameters:
    eh: EventHubCaptureExtractor for the eventhub data (raw)
    dh: DeltaHandle for the target delta table (bronze)

    Returns:
    Processed datasets of the super Orchestrator class
    """

    def __init__(self, eh: EventHubCaptureExtractor, dh: DeltaHandle):
        super().__init__()
        self.eh = eh
        self.dh = dh

        # step 1,
        #  - get the highest partition from the delta table,
        #  - truncate that partition
        #  - read the capture files from that partition
        self.extract_from(EhJsonToDeltaExtractor(eh, dh))

        # Step 2,
        # Transform the schema into a readable format
        self.transform_with(EhToDeltaBronzeTransformer(target_dh=dh))

        # the method filter_with can be used to insert any number of transformers here

        # final step,
        # append the rows to the delta table.
        self._loader = SimpleLoader(dh, mode="append")
        self.load_into(self._loader)

    @classmethod
    def from_tc(cls, eh_id: str, dh_id: str):
        return cls(
            eh=EventHubCaptureExtractor.from_tc(eh_id), dh=DeltaHandle.from_tc(dh_id)
        )

    def filter_with(self, etl: EtlBase):
        """Additional filters to execute before loading."""
        loader = self.steps.pop()

        # the following will fail if additional steps have been added.
        assert self._loader is loader, "unexpected change in etl steps"

        self.transform_with(etl)
        self.load_into(loader)
