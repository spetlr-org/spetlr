from typing import Dict, List
from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from atc.etl import Loader
from atc.etl.types import dataset_group


class MockLoader(Loader, MagicMock):
    def __init__(
        self,
        dataset_input_key: str = None,
        dataset_input_key_list: List[str] = None,
        *args,
        **kwargs
    ):
        MagicMock.__init__(
            self,
            dataset_input_key=dataset_input_key,
            dataset_input_key_list=dataset_input_key_list,
            *args,
            **kwargs
        )
        self.saved: Dict[str, DataFrame] = {}

    def save(self, df: DataFrame) -> None:
        self.saved = {"single": df}

    def save_many(self, datasets: dataset_group) -> None:
        self.saved = datasets

    def getDf(self) -> DataFrame:
        return self.saved["single"]
