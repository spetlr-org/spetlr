from typing import List

from atc_tools.testing import DataframeTestCase, TestHandle
from atc_tools.time import dt_utc

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.etl.extractors.incremental_extractor import IncrementalExtractor
from atc.utils import DataframeCreator
from tests.cluster.delta import extras
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor


class IncrementalExtractorTests(DataframeTestCase):
    source_id = "IncrementalExtractorDummySource"
    target_id = "IncrementalExtractorDummyTarget"

    date_row1 = dt_utc(2021, 1, 1, 10, 50)  # 1st of january 2021, 10:50
    date_row2 = dt_utc(2021, 1, 1, 10, 55)  # 1st of january 2021, 10:55
    date_row2Inc = dt_utc(2021, 1, 1, 10, 56)  # 1st of january 2021, 10:56
    date_row3 = dt_utc(2021, 1, 1, 11, 00)  # 1st of january 2021, 11:00

    row1 = (1, "string1", date_row1)

    row2 = (2, "string2", date_row2)

    row2Inc = (22, "string2Inc", date_row2Inc)

    row3 = (3, "String3", date_row3)

    date_row4 = dt_utc(2021, 1, 1, 10, 58)  # 1st of january 2021, 10:58 # Late arrival
    date_row5 = dt_utc(2021, 1, 1, 12, 00)  # 1st of january 2021, 12:00
    date_row6 = dt_utc(2021, 1, 1, 12, 5)  # 1st of january 2021, 12:05

    row4 = (
        4,
        "Body4",
        date_row4.date(),
        date_row4,
        None,
        "Properties4",
        "SystemProperties4",
    )
    row5 = (
        5,
        "Body5",
        date_row5.date(),
        date_row5,
        None,
        "Properties5",
        "SystemProperties5",
    )
    row6 = (
        6,
        "Body6",
        date_row6.date(),
        date_row6,
        None,
        "Properties6",
        "SystemProperties6",
    )

    source1 = [row1, row2, row3]
    source1Inc = [row1, row2Inc, row3]

    target1 = [row1, row2]

    target1Inc = [row1, row2]

    extract1Inc = [
        row2Inc,
        row3,
    ]

    dummy_columns: List[str] = ["col1", "col2", "col3", "timecol"]

    dummy_schema = None
    target_dh_dummy: DeltaHandle = None
    source_dh_dummy: DeltaHandle = None

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().add_resource_path(extras)
        Configurator().set_debug()

        cls.target_dh_dummy = DeltaHandle.from_tc("IncrementalExtractorDummyTarget")
        cls.source_dh_dummy = DeltaHandle.from_tc("IncrementalExtractorDummySource")

        SparkSqlExecutor().execute_sql_file("incremental-extract-test")

        cls.dummy_schema = cls.target_dh_dummy.read().schema

        # make sure target is empty
        df_empty = DataframeCreator.make_partial(cls.dummy_schema, [], [])
        cls.target_dh_dummy.overwrite(df_empty)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("IncrementalExtractorDb").drop_cascade()

    def test_01_can_perform_incremental_on_empty(self):
        """Target is empty. Source has data."""

        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.source1
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, []
            )
        )

        extractor = IncrementalExtractor(
            handleSource=source_test_handle,
            handleTarget=target_test_handle,
            timeCol="timecol",
            dataset_key="source",
        )

        df_result = extractor.read()

        self.assertDataframeMatches(df_result, None, self.source1)

    def test_02_can_extract_incremental(self):
        source_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.source1Inc
            )
        )

        target_test_handle = TestHandle(
            provides=DataframeCreator.make_partial(
                self.dummy_schema, self.dummy_columns, self.target1Inc
            )
        )

        extractor = IncrementalExtractor(
            handleSource=source_test_handle,
            handleTarget=target_test_handle,
            timeCol="timecol",
            dataset_key="source",
        )

        df_extract = extractor.read()

        self.assertDataframeMatches(df_extract, None, self.extract1Inc)

    def test_03_can_perform_merge(self):
        """The target table is already filled from before."""
        existing_rows = self.target_dh_dummy.read().collect()
        self.assertEqual(3, len(existing_rows))

        loader = UpsertLoader(handle=self.target_dh_dummy, join_cols=self.join_cols)

        df_source = DataframeCreator.make_partial(
            self.dummy_schema, self.dummy_columns, self.data3
        )

        loader.save(df_source)

        self.assertDataframeMatches(self.target_dh_dummy.read(), None, self.data4)
