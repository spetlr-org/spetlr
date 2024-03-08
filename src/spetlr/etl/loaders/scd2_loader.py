from typing import List

from pyspark.sql import DataFrame

from spetlr.delta import DeltaHandle
from spetlr.etl import Loader
from spetlr.etl.transformers import ValidFromToTransformer
from spetlr.functions import get_unique_tempview_name
from spetlr.utils import GetMergeStatement
from spetlr.utils.Md5HashColumn import Md5HashColumn


class ValidFromToUpsertLoader(Loader):
    def __init__(
        self,
        sink_handle: DeltaHandle,  # Work for now only for delta
        *,
        join_cols: List[str] = None,
        time_col: str = "TimeCol",
        hash_value_col: str = "HashValue",
        dataset_input_keys: List[str] = None,
    ):
        """
        Class Alias: SCD2UpsertLoader

        sink_handle: A DeltaHandle that points to the sink delta table
        join_cols: The joining columns are used for identifying which
                   rows in the sink table that will
                   be affected by the incoming SCD2 update.
                   These rows (with the given join_cols)
                   Should have "rerunned" their SCD2 values
        time_col: The column used for creating the SCD2 logic.
        hash_value_col: The name of the column where the md5 encoding of the data


        """
        super().__init__(dataset_input_keys=dataset_input_keys)
        self.sink_handle = sink_handle
        self.join_cols = join_cols
        self.time_col = time_col
        self.hash_value_col = hash_value_col

    def save(self, df: DataFrame) -> None:

        _scd2_cols = ["ValidFrom", "ValidTo", "IsCurrent"]
        _cols_without_scd2 = [col for col in df.columns if col not in _scd2_cols]

        # assert _cols_without_scd2 = sink schema cols

        # Generate md5 hash value
        # This is used for the merge statement
        # The hash-value will be the primary key for merging
        df = Md5HashColumn(df, colName=self.hash_value_col, cols_to_exclude=_scd2_cols)

        # Adding the hash-value column to the
        # list over non-scd2 columns
        # This will be used when unioning the
        # input dataframe and the relevant data from the sink table
        _cols_without_scd2 = _cols_without_scd2 + [self.hash_value_col]

        # Selecting data from sink table
        # it is neccesary to get the sink data.
        # Find the rows that have the same join column values
        # as the ones from the source table
        # These values could potentially be updated
        # Some IsCurrent=true rows could potential be IsCurrent=false
        # also, if any data is late arrivals
        # the ValidTo and ValidFrom could also get updated
        df_sink_data = self.sink_handle.read()

        # This ensures, that the sink table is filtered
        # such that is only contains rows
        # that matches with the values in the df
        # values found in the join columns
        for col in self.join_cols:
            _filter_id = df.select(col).distinct()
            df_sink_data = df_sink_data.join(_filter_id, on=col)

        # The affected sink data will now be added to the
        # incoming dataframe.
        df_ready = df.select(_cols_without_scd2).unionByName(
            df_sink_data.select(_cols_without_scd2)
        )

        # Generate the SCD2 data
        df = ValidFromToTransformer(
            time_col=self.time_col, wnd_cols=self.join_cols
        ).process(df_ready)

        # Save the data as a view such that the merge statement
        # can pick it up
        temp_view_name = get_unique_tempview_name()
        df.createOrReplaceGlobalTempView(temp_view_name)  # The correct solution

        # Create the upsert (merge) statement
        _sink_table_name = self.sink_handle.get_tablename()
        _sink_cols = self.sink_handle.read().columns

        _merge_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=_sink_table_name,
            source_table_name="global_temp." + temp_view_name,
            insert_cols=_sink_cols,
            join_cols=[self.hash_value_col],
            update_cols=_sink_cols,
        )

        df._jdf.sparkSession().sql(_merge_statement)


SCD2UpsertLoader = ValidFromToUpsertLoader
