from unittest.mock import Mock, patch

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from spetlrtools.testing import DataframeTestCase

from spetlr.etl import Transformer
from spetlr.etl.extractors import IncrementalExtractor, SimpleExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.exceptions import MissingUpsertJoinColumns
from spetlr.orchestrators import EhToDeltaSilverOrchestrator
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)
from spetlr.spark import Spark


class EhToDeltaSilverOrchestratorTests(DataframeTestCase):
    def test_01_default_upsert(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        with patch.object(IncrementalExtractor, "read", return_value=test_df) as p0:
            with patch.object(
                EhJsonToDeltaTransformer, "process", return_value=test_df
            ) as p1:
                with patch.object(SimpleLoader, "save", return_value=test_df) as p2:
                    orchestrator = EhToDeltaSilverOrchestrator(
                        dh_source=dh_source_mock,
                        dh_target=dh_target_mock,
                        upsert_join_cols=["id"],
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()

    def test_02_input_upsert(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        with patch.object(IncrementalExtractor, "read", return_value=test_df) as p0:
            with patch.object(
                EhJsonToDeltaTransformer, "process", return_value=test_df
            ) as p1:
                with patch.object(SimpleLoader, "save", return_value=test_df) as p2:
                    orchestrator = EhToDeltaSilverOrchestrator(
                        dh_source=dh_source_mock,
                        dh_target=dh_target_mock,
                        mode="upsert",
                        upsert_join_cols=["id"],
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()

    def test_03_upsert_missing_upsert_cols(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        with patch.object(IncrementalExtractor, "read", return_value=test_df) as p0:
            with patch.object(
                EhJsonToDeltaTransformer, "process", return_value=test_df
            ) as p1:
                with patch.object(SimpleLoader, "save", return_value=test_df) as p2:
                    with self.assertRaises(MissingUpsertJoinColumns) as cm:
                        orchestrator = EhToDeltaSilverOrchestrator(
                            dh_source=dh_source_mock,
                            dh_target=dh_target_mock,
                            mode="upsert",
                        )
                        orchestrator.execute()

                        p0.assert_called_once()
                        p1.assert_called_once()
                        p2.assert_called_once()
                        cm.exception = "You must specify upsert_join_cols"

    def test_04_w_transformer(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        class TestFilter(Transformer):
            def process(self, df: DataFrame) -> DataFrame:
                return df.withColumn("Test", f.lit("Test"))

        class TestOrchestrator(EhToDeltaSilverOrchestrator):
            def __init__(self):
                super().__init__(
                    dh_source=dh_source_mock,
                    dh_target=dh_target_mock,
                    upsert_join_cols=["id"],
                )
                self.filter_with(TestFilter())

        with patch.object(
            EhJsonToDeltaTransformer, "process", return_value=test_df
        ) as p1:
            with patch.object(TestFilter, "process", return_value=test_df) as p3:
                orchestrator = TestOrchestrator()
                orchestrator.execute()

                dh_source_mock.read.assert_called_once()
                p1.assert_called_once()
                dh_target_mock.upsert.assert_called_once()
                p3.assert_called_once()

    def test_05_append(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        with patch.object(IncrementalExtractor, "read", return_value=test_df) as p0:
            with patch.object(
                EhJsonToDeltaTransformer, "process", return_value=test_df
            ) as p1:
                with patch.object(SimpleLoader, "save", return_value=test_df) as p2:
                    orchestrator = EhToDeltaSilverOrchestrator(
                        dh_source=dh_source_mock,
                        dh_target=dh_target_mock,
                        mode="append",
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()

    def test_06_overwrite(self):
        test_df = Spark.get().createDataFrame(
            [], schema="id string, EnqueuedTimestamp timestamp"
        )
        dh_source_mock = Mock()
        dh_source_mock.read = Mock(return_value=test_df)
        dh_target_mock = Mock()
        dh_target_mock.read = Mock(return_value=test_df)

        with patch.object(SimpleExtractor, "read", return_value=test_df) as p0:
            with patch.object(
                EhJsonToDeltaTransformer, "process", return_value=test_df
            ) as p1:
                with patch.object(SimpleLoader, "save", return_value=test_df) as p2:
                    orchestrator = EhToDeltaSilverOrchestrator(
                        dh_source=dh_source_mock,
                        dh_target=dh_target_mock,
                        mode="overwrite",
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()
