from datetime import datetime
from typing import List

from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl.loaders.DeleteDataLoader import DeleteDataLoader
from spetlr.utils import DataframeCreator
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class UpsertLoaderTests(DataframeTestCase):
    target_id = "DeleteDataLoaderDummy"

    input_data = [
        (43, 43.0, "42", datetime(2023, 4, 14, 0, 0, 0)),
        (42, 42.0, "42", datetime(2023, 4, 13, 0, 0, 0)),
        (41, 41.0, "42", datetime(2023, 4, 12, 0, 0, 0)),
        (40, 40.0, "42", datetime(2023, 4, 11, 0, 0, 0)),
    ]

    data1 = [
        (43, 43.0, "42", datetime(2023, 4, 14, 0, 0, 0)),
        (42, 42.0, "42", datetime(2023, 4, 13, 0, 0, 0)),
        (41, 41.0, "42", datetime(2023, 4, 12, 0, 0, 0)),
    ]

    data2 = [
        (42, 42.0, "42", datetime(2023, 4, 13, 0, 0, 0)),
        (41, 41.0, "42", datetime(2023, 4, 12, 0, 0, 0)),
    ]

    data3 = [
        (42, 42.0, "42", datetime(2023, 4, 13, 0, 0, 0)),
    ]

    data4 = []

    dummy_columns: List[str] = ["col1", "col2", "col3", "col4"]

    dummy_schema = None
    target_dh_dummy: DeltaHandle = None

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().add_resource_path(extras)
        Configurator().set_debug()

        cls.target_dh_dummy = DeltaHandle.from_tc("DeleteDataLoaderDummy")

        SparkSqlExecutor().execute_sql_file("deletedataloader-test")

        cls.dummy_schema = cls.target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(cls.dummy_schema, [], [])
        cls.target_dh_dummy.overwrite(df_empty)

        # add specific data
        cls.target_dh_dummy.write_or_append(
            DataframeCreator.make_partial(
                cls.dummy_schema, cls.dummy_columns, cls.data1
            )
        )

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("UpsertLoaderDb").drop_cascade()

    def test_01_can_delete_int_lt(self):
        loader = DeleteDataLoader(
            handle=self.target_dh_dummy,
            comparison_col="col1",
            comparison_limit=41,
            comparison_operator="<",
        )

        loader.save(None)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data1)

    def test_02_can_delete_float_gt(self):
        loader = DeleteDataLoader(
            handle=self.target_dh_dummy,
            comparison_col="col2",
            comparison_limit=42.0,
            comparison_operator=">",
        )

        loader.save(None)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data2)

    def test_03_can_delete_string_eq(self):
        loader = DeleteDataLoader(
            handle=self.target_dh_dummy,
            comparison_col="col3",
            comparison_limit="42",
            comparison_operator="=",
        )

        loader.save(None)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data3)

    def test_4_can_delete_timestamp(self):
        loader = DeleteDataLoader(
            handle=self.target_dh_dummy,
            comparison_col="col4",
            comparison_limit=datetime(2023, 4, 14, 0, 0, 0),
            comparison_operator="<",
        )

        loader.save(None)
        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data4)
