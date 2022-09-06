from pyspark.sql import DataFrame

from atc.config_master import TableConfigurator
from atc.etl import Loader, dataset_group
from atc.etl.loaders.UpsertLoaderParameters import UpsertLoaderParameters
from atc.functions import get_unique_tempview_name
from atc.spark import Spark
from atc.utils.CheckDfMerge import CheckDfMerge
from atc.utils.GetMergeStatement import GetMergeStatement


class UpsertLoader(Loader):
    def __init__(self, params: UpsertLoaderParameters):
        super().__init__()
        self.params = params

    def do_simple_save(self, df: DataFrame, incremental_load: bool = None):
        incremental_load = incremental_load or self.params.incremental_load

        mode = "append" if incremental_load else "overwrite"

        self.params.target_dh.write_or_append(df, mode=mode)
        print(
            "Incremental Base - incremental load with append"
            if incremental_load
            else "Incremental Base - full load with append"
        )
        return df

    def save_many(self, datasets: dataset_group) -> None:
        pass

    def save(self, df_source: DataFrame):
        if df_source is None:
            return df_source

        df = df_source

        # check null keys in our dataframe.
        any_null_keys = len(
            df.filter(
                " OR ".join(f"({col} is NULL)" for col in self.params.join_cols)
            ).take(1)
        )

        if any_null_keys:
            print(
                "Null keys found in input dataframe. "
                "Rows will be discarded before load."
            )
            df = df.filter(
                " AND ".join(f"({col} is NOT NULL)" for col in self.params.join_cols)
            )

        # Load data from the target table for the purpose of incremental load
        if not self.params.incremental_load:
            return self.do_simple_save(df)

        df_target = self.params.target_dh.read()

        # If the target is empty, always do faster full load
        if len(df_target.take(1)) == 0:
            return self.do_simple_save(df, incremental_load=False)

        # Find records that need to be updated in the target (happens seldom)

        # Define the column to be used for checking for new rows
        # Checking the null-ness of one right row is sufficient to mark the row as new,
        # since null keys are disallowed.

        df, merge_required = CheckDfMerge(
            df=df,
            df_target=df_target,
            join_cols=self.params.join_cols,
            avoid_cols=[],
        )

        if not merge_required:
            return self.do_simple_save(df)

        temp_view_name = get_unique_tempview_name()
        df.createOrReplaceTempView(temp_view_name)

        target_table_name = TableConfigurator().table_name(self.params.target_id)
        non_join_cols = [col for col in df.columns if col not in self.params.join_cols]

        merge_sql_statement = GetMergeStatement(
            merge_statement_type="delta",
            target_table_name=target_table_name,
            source_table_name=temp_view_name,
            join_cols=self.params.join_cols,
            insert_cols=df.columns,
            update_cols=non_join_cols,
            special_update_set="",
        )

        Spark.get().sql(merge_sql_statement)

        print("Incremental Base - incremental load with merge")

        return df
