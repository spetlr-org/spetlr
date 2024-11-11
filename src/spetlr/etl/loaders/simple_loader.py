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
        overwritePartitions: bool = None,
    ):
        super().__init__(dataset_input_keys=dataset_input_keys)
        self.mode = mode.lower()
        self.handle = handle
        self.join_cols = join_cols
        self.overwriteSchema = overwriteSchema
        self.mergeSchema = mergeSchema
        self.overwritePartitions = overwritePartitions

    def save(self, df: DataFrame) -> None:
        args = dict(
            overwriteSchema=self.overwriteSchema,
            mergeSchema=self.mergeSchema,
            overwritePartitions=self.overwritePartitions,
        )
        # remove those arguments that are None. Those are not passed to handle
        args = {k: v for k, v in args.items() if v is not None}

        if self.mode == "overwrite":
            self.handle.overwrite(df, **args)
        elif self.mode == "upsert":
            self.handle.upsert(df, self.join_cols)
        else:
            self.handle.append(df, **args)
