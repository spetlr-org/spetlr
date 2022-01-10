from typing import List

from pyspark.sql import DataFrame

from .types import EtlBase, dataset_group


class Orchestrator:
    """
    It is up to the user of this library that extractors,
    transformers and loaders live up to their names and are not
    used in a wrong order.
    """

    def __init__(self):
        self.steps: List[EtlBase] = []

    def step(self, etl: EtlBase) -> "Orchestrator":
        self.steps.append(etl)
        return self

    # these are just synonyms for readability
    extract_from = step
    transform_with = step
    load_into = step

    def execute(self) -> DataFrame:
        datasets: dataset_group = {}
        for step in self.steps:
            datasets = step.etl(datasets)

        if len(datasets) == 1:
            return next(iter(datasets.values()))
        raise AssertionError("Multiple datasets in play at the end of orchestration.")
