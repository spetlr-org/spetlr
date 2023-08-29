import unittest

from pyspark.sql.types import IntegerType, StructField, StructType
from spetlrtools.testing import DataframeTestCase, TestHandle

from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.log import LogOrchestrator
from spetlr.etl.log.log_transformers import CountLogTransformer, NullLogTransformer
from spetlr.utils import DataframeCreator


class TestLogTransformer(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema = StructType(
            [
                StructField("col_1", IntegerType(), True),
                StructField("col_2", IntegerType(), True),
                StructField("col_3", IntegerType(), True),
            ]
        )

        data = [
            (1, 1, 1),
            (2, 2, 2),
            (3, 3, 3),
            (4, 4, 4),
            (5, 5, 5),
        ]

        cls.df = DataframeCreator.make(schema, data)

    def test_log_step_single_df_01(self) -> None:
        sink_log_handle = TestHandle()
        source_handle = TestHandle(self.df)

        log_oc = LogOrchestrator(
            handles=[sink_log_handle],
        )

        log_oc.extract_from(
            SimpleExtractor(
                handle=source_handle,
                dataset_key="df_test",
            )
        )

        log_oc.log_with(
            CountLogTransformer(
                log_name="log_test",
                dataset_input_keys=["df_test"],
            )
        )

        log_oc.execute()

        df_log = sink_log_handle.appended

        self.assertIsNotNone(df_log)
        self.assertEqual(df_log.count(), 1)

    def test_log_step_multiple_dfs_02(self) -> None:
        sink_log_handle = TestHandle()
        source_handle = TestHandle(self.df)

        log_oc = LogOrchestrator(
            handles=[sink_log_handle],
        )

        log_oc.extract_from(
            SimpleExtractor(
                handle=source_handle,
                dataset_key="df_test_1",
            )
        )

        log_oc.extract_from(
            SimpleExtractor(
                handle=source_handle,
                dataset_key="df_test_2",
            )
        )

        log_oc.log_with(
            CountLogTransformer(
                log_name="log_test_count",
                dataset_input_keys=["df_test_1", "df_test_2"],
                consume_inputs=False,
            )
        )

        log_oc.log_with(
            NullLogTransformer(
                log_name="log_test_null",
                column_name="col_2",
                dataset_input_keys=["df_test_1", "df_test_2"],
                consume_inputs=False,
            )
        )

        log_oc.execute()

        df_log = sink_log_handle.appended

        self.assertIsNotNone(df_log)
        self.assertEqual(df_log.count(), 4)

    def test_log_step_multiple_dfs_different_order_03(self) -> None:
        sink_log_handle = TestHandle()
        source_handle = TestHandle(self.df)

        log_oc = LogOrchestrator(
            handles=[sink_log_handle],
        )

        log_oc.extract_from(
            SimpleExtractor(
                handle=source_handle,
                dataset_key="df_test_1",
            )
        )

        log_oc.log_with(
            CountLogTransformer(
                log_name="log_test_count",
                dataset_input_keys=["df_test_1"],
                consume_inputs=False,
            )
        )

        log_oc.extract_from(
            SimpleExtractor(
                handle=source_handle,
                dataset_key="df_test_2",
            )
        )

        log_oc.log_with(
            NullLogTransformer(
                log_name="log_test_null",
                column_name="col_2",
                dataset_input_keys=["df_test_1", "df_test_2"],
                consume_inputs=False,
            )
        )

        log_oc.log_with(
            CountLogTransformer(
                log_name="log_test_count",
                dataset_input_keys=["df_test_2"],
                consume_inputs=False,
            )
        )

        log_oc.execute()

        df_log = sink_log_handle.appended

        self.assertIsNotNone(df_log)
        self.assertEqual(df_log.count(), 4)


if __name__ == "__main__":
    unittest.main()
