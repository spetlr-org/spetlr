from atc.delta import DeltaHandle
from atc.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from atc.etl import EtlBase, Orchestrator
from atc.etl.loaders import SimpleLoader
from atc.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import EhJsonToDeltaExtractor
from atc.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)


class EhJsonToDeltaOrchestrator(Orchestrator):
    def __init__(self, eh: EventHubCaptureExtractor, dh: DeltaHandle):
        super().__init__()
        self.eh = eh
        self.dh = dh

        # step 1,
        #  - get the highest partition from the delta table,
        #  - truncate that partition
        #  - read the capture files from that partition
        self.extract_from(EhJsonToDeltaExtractor(eh, dh))

        # step 2,
        #  - use the target schema to select what to copy from capture files
        #  - anything that is not in the source df is used to unpack the body json
        self.transform_with(EhJsonToDeltaTransformer(eh, dh))

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
