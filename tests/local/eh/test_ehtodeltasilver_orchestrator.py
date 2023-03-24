from unittest.mock import Mock, patch

from atc_tools.testing import DataframeTestCase
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from atc.etl import Transformer
from atc.etl.extractors import IncrementalExtractor, SimpleExtractor
from atc.etl.loaders import SimpleLoader
from atc.etl.loaders.UpsertLoader import UpsertLoader
from atc.orchestrators import EhToDeltaSilverOrchestrator
from atc.orchestrators.ehjson2delta.EhJsonToDeltaTransformer import (
    EhJsonToDeltaTransformer,
)
from atc.spark import Spark


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
                with patch.object(UpsertLoader, "save", return_value=test_df) as p2:
                    orchestrator = EhToDeltaSilverOrchestrator(
                        dh_source=dh_source_mock,
                        dh_target=dh_target_mock,
                        upsert_join_cols=["id"],
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()

    def test_02_w_transformer(self):
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

    def test_03_append(self):
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
                        mode="append",
                    )
                    orchestrator.execute()

                    p0.assert_called_once()
                    p1.assert_called_once()
                    p2.assert_called_once()
