import warnings
from typing import List

from .types import EtlBase, dataset_group


class Orchestrator(EtlBase):
    """
    It is up to the user of this library that extractors,
    transformers and loaders live up to their names and are not
    used in a wrong order.
    """

    def __init__(self, suppress_composition_warning=False):
        super().__init__()
        self.steps: List[EtlBase] = []
        self.suppress_composition_warning = suppress_composition_warning

    def step(self, etl: EtlBase) -> "Orchestrator":
        self.steps.append(etl)
        return self

    # these are just synonyms for readability
    extract_from = step
    transform_with = step
    load_into = step

    def etl(self, inputs: dataset_group = None) -> dataset_group:
        inputs = inputs or {}

        # make a shallow copy of the inputs for waring after the first step
        datasets = inputs.copy()
        if not self.steps:
            raise NotImplementedError("The orchestrator has no steps.")

        # treat the fist step differently to warn in case the input was not handled
        datasets = self.steps[0].etl(datasets)
        if len(inputs) and (len(inputs) + 1 == len(datasets)):
            # There were inputs to the orchestrator,
            # and the first step did not clean them up.
            # expect problems.

            # let's double check:
            if (
                # step 1 has retained all input keys
                set(inputs.keys()).issubset(set(datasets.keys()))
                and
                # and has not changed their values
                all(id(inputs[k]) == id(datasets[k]) for k in inputs.keys())
                and not self.suppress_composition_warning
            ):
                warnings.warn(
                    "You used inputs to the orchestrator, "
                    "and the first step did not make use of them. "
                    "Expect problems in your etl pipeline. To avoid this, "
                    "write extractors that clean up in self.previous_extractions"
                )

        for step in self.steps[1:]:
            datasets = step.etl(datasets)
        return datasets

    execute = etl
