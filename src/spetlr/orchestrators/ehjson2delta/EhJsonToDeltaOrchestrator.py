from spetlr.delta import DeltaHandle
from spetlr.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from spetlr.etl import EtlBase, Orchestrator
from spetlr.etl.loaders import SimpleLoader
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import (
    EhJsonToDeltaExtractor,
)
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)


class EhJsonToDeltaOrchestrator(Orchestrator):
    def __init__(
        self,
        eh: EventHubCaptureExtractor,
        dh: DeltaHandle,
        *,
        case_sensitive: bool = True,
    ):
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
        self.transform_with(
            EhJsonToDeltaTransformer(target_dh=dh, case_sensitive=case_sensitive)
        )

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
