from typing import List, Union

from pyspark.sql import DataFrame

from spetlr.etl import Loader
from spetlr.etl.loaders import Appendable, Overwritable, Upsertable


class SimpleLoader(Loader):
    def __init__(
        self,
        handle: Union[Overwritable, Appendable, Upsertable],
        *,
        mode: str = "overwrite",
        join_cols: List[str] = None,
        dataset_input_keys: List[str] = None,
        mergeSchema: bool = None,
        overwriteSchema: bool = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)
        self.mode = mode.lower()
        self.handle = handle
        self.join_cols = join_cols
        self.overwriteSchema = overwriteSchema
        self.mergeSchema = mergeSchema

    def save(self, df: DataFrame) -> None:
        if self.mode == "overwrite":
            self.handle.overwrite(
                df, overwriteSchema=self.overwriteSchema, mergeSchema=self.mergeSchema
            )
        elif self.mode == "upsert":
            self.handle.upsert(df, self.join_cols)
        else:
            self.handle.append(df, mergeSchema=self.mergeSchema)
