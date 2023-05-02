import warnings
from typing import List, Union

from .types import EtlBase, MLBase, MLModelBase, dataset_group
from .orchestrator import Orchestrator

class MLOrchestrator():
    """
    It is up to the user of this library that extractors,
    transformers and loaders live up to their names and are not
    used in a wrong order.
    """

    def __init__(self, suppress_composition_warning=False):
        super().__init__()
        self.steps: List[Union[EtlBase,MLBase,MLModelBase]] = []
        self.suppress_composition_warning = suppress_composition_warning

        self.pipeline = empty pipeline

    def step(self, step: Union[EtlBase,MLBase,MLModelBase]) -> "MLOrchestrator":
        self.steps.append(step)
        return self

    # these are just synonyms for readability
    extract_from = step
    transform_with = step
    load_into = step
    

    def etl(self, inputs: dataset_group = None, input_pipeline: pipeline_stage_type = None) -> (dataset_group, pipeline_stage_type):
        inputs = inputs or {}

        # make a shallow copy of the inputs for waring after the first step
        datasets = inputs.copy()
        if not self.steps:
            raise NotImplementedError("The orchestrator has no steps.")

        # Define the empty starting pipeline
        pipeline = input_pipeline or Pipeline(stages=[])

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
            if isinstance(step, EtlBase):
                datasets = step.etl(datasets)
            if isinstance(step, MLBase):
                pipeline = step.pipe(pipeline)
            if isinstance(step, MLModelBase):
                pipeline = step.pipe(datasets, pipeline)

        return datasets, pipeline

    execute = etl
