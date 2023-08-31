from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Loader, dataset_group
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.tables import TableHandle


class UpsertLoaderStreaming(Loader):
    def __init__(
        self,
        handle: TableHandle,
        options_dict: dict = None,
        trigger_type: str = None,
        trigger_time_seconds: int = None,
        query_name: str = None,
        checkpoint_path: str = None,
        await_termination: bool = False,
        upsert_join_cols: List[str] = None,
    ):
        super().__init__()

        self._loader = StreamLoader(
            loader=SimpleLoader(
                handle=handle, join_cols=upsert_join_cols, mode="upsert"
            ),
            options_dict=options_dict,
            trigger_type=trigger_type,
            trigger_time_seconds=trigger_time_seconds,
            outputmode="update",
            query_name=query_name,
            checkpoint_path=checkpoint_path,
            await_termination=await_termination,
        )

    def save_many(self, datasets: dataset_group) -> None:
        raise NotImplementedError()

    def save(self, df: DataFrame) -> None:
        """Upserts a single dataframe to the target table."""
        self._loader.save(df)
