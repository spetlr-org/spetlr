from typing import List

from atc_tools.testing import DataframeTestCase

from atc.config_master import TableConfigurator
from atc.delta import DbHandle, DeltaHandle
from atc.etl.loaders.UpsertLoader import UpsertLoader
from atc.etl.loaders.UpsertLoaderParameters import UpsertLoaderParameters
from atc.utils import DataframeCreator
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class UpsertLoaderTests(DataframeTestCase):

    target_id = "UpsertLoaderDummy"

    join_cols = ["col1", "col2"]

    data1 = [
        (1, 2, "foo"),
        (3, 4, "bar"),
    ]
    data2 = [
        (5, 6, "foo"),
        (7, 8, "bar"),
    ]
    data3 = [
        (1, 2, "baz"),
    ]
    data4 = [(5, 6, "boo"), (5, 7, "spam")]
    # data5 is the merge result of data2 + data3 + data4
    data5 = [(1, 2, "baz"), (5, 6, "boo"), (5, 7, "spam"), (7, 8, "bar")]

    dummy_columns: List[str] = ["col1", "col2", "col3"]

    dummy_schema = None
    target_dh_dummy: DeltaHandle = None

    @classmethod
    def setUpClass(cls) -> None:
        TableConfigurator().add_resource_path(extras)
        TableConfigurator().set_debug()

        cls.target_dh_dummy = DeltaHandle.from_tc("UpsertLoaderDummy")

        SparkSqlExecutor().execute_sql_file("upsertloader-test")

        cls.dummy_schema = cls.target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(cls.dummy_schema, [], [])
        cls.target_dh_dummy.overwrite(df_empty)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("UpsertLoaderDb").drop_cascade()

    def test_01_can_perform_incremental_on_empty(self):
        params = UpsertLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = UpsertLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data1
        )

        loader.save(df_source)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_perform_full_over_existing(self):
        """The target table is already filled from before."""
        self.assertEqual(2, len(self.target_dh_dummy.read().collect()))

        params = UpsertLoaderParameters(
            incremental_load=False, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = UpsertLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data2
        )

        loader.save(df_source)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data2)

    def test_03_can_perform_incremental_append(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(2, len(existing_rows))

        params = UpsertLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = UpsertLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data3
        )

        loader.save(df_source)

        self.assertDataframeMatches(
            self.target_dh_dummy.read(), None, self.data2 + self.data3
        )

    def test_04_can_perform_merge(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(3, len(existing_rows))

        params = UpsertLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = UpsertLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data4
        )

        loader.save(df_source)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data5)
