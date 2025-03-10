# Defines input parameters for a cached Loader

from typing import List


class CachedLoaderParameters:
    def __init__(
        self,
        cache_table_name: str,
        key_cols: List[str],
        cache_id_cols: List[str] = None,
        *,
        do_nothing_if_more_rows_than: int = None,
        provisional_markup_step: bool = True,
        retry_cache_writes: int = 0,
        retry_cache_wait_seconds=10,
    ):
        """
        Args:
            cache_table_name: The table that holds the cache
            key_cols: the set of columns that form the primary key for a row
            cache_id_cols: These columns, added by the write operation, will be saved
                in the cache to identify e.g. the written batch.
            do_nothing_if_more_rows_than: if the input data set contains more rows than
               the specified number of rows, nothing will be written or deleted.
               Instead, the method too_many_rows() will be called.
            provisional_markup_step: if true (default) a provisional cache update
               is applied before the write or delete operation is performed, and
               rolled back afterward before applying the actual cache update.
               While this protects against data corruption, it is also slower and can
               be turned off.

        The table cache_table_name must exist and must have the following schema:
        (
            [key_cols column definitions],
            rowHash INTEGER,
            loadedTime TIMESTAMP,
            deletedTime TIMESTAMP,
            [cache_id_cols definitions, (if used)]
        )
        """

        self.cache_id_cols = cache_id_cols or []
        self.cache_table_name = cache_table_name
        self.key_cols = key_cols
        if not key_cols:
            raise ValueError("key columns must be provided")

        self.rowHash = "rowHash"
        self.loadedTime = "loadedTime"
        self.deletedTime = "deletedTime"

        self.do_nothing_if_more_rows_than = do_nothing_if_more_rows_than
        self.provisional_markup_step = provisional_markup_step
        self.retry_cache_writes = retry_cache_writes
        self.retry_cache_wait_seconds = retry_cache_wait_seconds
