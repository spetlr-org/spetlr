from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.utils import DropOldestDuplicates


class DropOldestDuplicatesTransformer(Transformer):
    def __init__(self, *, cols: List[str], orderByColumn: str):
        super().__init__()
        self.cols = cols
        self.orderByColumn = orderByColumn

    def process(self, df: DataFrame) -> DataFrame:
        return DropOldestDuplicates(
            df=df, cols=self.cols, orderByColumn=self.orderByColumn
        )
