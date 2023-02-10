from typing import List

from atc_tools.testing import DataframeTestCase

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.utils import DataframeCreator
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class DeltaUpsertTests(DataframeTestCase):
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
        Configurator().add_resource_path(extras)
        Configurator().set_debug()

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
        """The target table is empty at beginning."""
        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data1
        )

        self.target_dh_dummy.upsert(df_source, join_cols=self.join_cols)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_perform_overwrite_over_existing(self):
        """The target table is already filled from before.
        This test does not test .upsert() logic,
        but ensures that test 03 resembles an upsert after a full load.
        If one needs to make an full load, use the .overwrite() method"""
        self.assertEqual(2, len(self.target_dh_dummy.read().collect()))

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data2
        )

        self.target_dh_dummy.overwrite(
            df_source,
        )

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data2)

    def test_03_can_perform_incremental_append(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(2, len(existing_rows))

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data3
        )

        self.target_dh_dummy.upsert(df_source, join_cols=self.join_cols)

        self.assertDataframeMatches(
            self.target_dh_dummy.read(), None, self.data2 + self.data3
        )

    def test_04_can_perform_merge(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(3, len(existing_rows))

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data4
        )

        self.target_dh_dummy.upsert(df_source, join_cols=self.join_cols)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data5)
