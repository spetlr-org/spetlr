from typing import List

from atc_tools.testing import DataframeTestCase

from atc.config_master import TableConfigurator
from atc.delta import DbHandle, DeltaHandle
from atc.etl.loaders.IncrementalLoader import IncrementalLoader
from atc.etl.loaders.IncrementalLoaderParameters import IncrementalLoaderParameters
from atc.utils import DataframeCreator
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class IncrementalBaseLoaderTests(DataframeTestCase):

    target_id = "IncrementalBaseDummy"

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
        TableConfigurator().set_debug()

        target_dh_dummy = DeltaHandle.from_tc("IncrementalBaseDummy")

        SparkSqlExecutor().execute_sql_file("incremental-base-create-test")

        dummy_schema = target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(dummy_schema, [], [])
        target_dh_dummy.overwrite(df_empty)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("IncrementalBaseDb").drop_cascade()

    def test_01_can_perform_incremental_on_empty(self):
        params = IncrementalLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = IncrementalLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data1
        )

        loader.save(df_source)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_perform_full_over_existing(self):
        """The target table is already filled from before."""
        self.assertEqual(2, len(self.target_dh_dummy.read().collect()))

        params = IncrementalLoaderParameters(
            incremental_load=False, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = IncrementalLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data2
        )

        loader.save(df_source)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data2)

    def test_03_can_perform_incremental_append(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(2, len(existing_rows))

        params = IncrementalLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = IncrementalLoader(params)

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

        params = IncrementalLoaderParameters(
            incremental_load=True, target_id=self.target_id, join_cols=self.join_cols
        )
        loader = IncrementalLoader(params)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data4
        )

        loader.save(df_source)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data5)
