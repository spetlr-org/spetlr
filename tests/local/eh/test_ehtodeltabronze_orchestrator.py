from unittest.mock import Mock, patch

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from spetlrtools.testing import DataframeTestCase

from spetlr.etl import Transformer
from spetlr.etl.loaders import SimpleLoader
from spetlr.orchestrators import EhToDeltaBronzeOrchestrator
from spetlr.orchestrators.eh2bronze.EhToDeltaBronzeTransformer import (
    EhToDeltaBronzeTransformer,
)
from spetlr.orchestrators.ehjson2delta.EhJsonToDeltaExtractor import (
    EhJsonToDeltaExtractor,
)
from spetlr.spark import Spark


class EhToDeltaBronzeOrchestratorTests(DataframeTestCase):
    def test_01_default(self):
        eh_mock = Mock()
        dh_mock = Mock()

        eh_mock.read = Mock(return_value=None)

        with patch.object(EhJsonToDeltaExtractor, "read", return_value=None) as p1:
            with patch.object(SimpleLoader, "save", return_value=None) as p2:
                with patch.object(
                    EhToDeltaBronzeTransformer, "process", return_value=None
                ) as p3:
                    orchestrator = EhToDeltaBronzeOrchestrator(eh=eh_mock, dh=dh_mock)
                    orchestrator.execute()

                    # EhJsonToDeltaExtractor should be called once
                    p1.assert_called_once()
                    # SimpleLoader should be called once
                    p2.assert_called_once()
                    # The transformer should be called once
                    p3.assert_called_once()

    def test_02_w_transformer(self):
        empty_df = Spark.get().createDataFrame([], schema="Id INTEGER")
        eh_mock = Mock()
        dh_mock = Mock()

        class TestFilter(Transformer):
            def process(self, df: DataFrame) -> DataFrame:
                return df.withColumn("Test", f.lit("Test"))

        class TestOrchestrator(EhToDeltaBronzeOrchestrator):
            def __init__(self):
                super().__init__(
                    eh=eh_mock,
                    dh=dh_mock,
                )
                self.filter_with(TestFilter())

        eh_mock.read = Mock(return_value=None)

        with patch.object(EhJsonToDeltaExtractor, "read", return_value=empty_df) as p1:
            with patch.object(SimpleLoader, "save", return_value=None) as p2:
                with patch.object(
                    EhToDeltaBronzeTransformer, "process", return_value=None
                ) as p3:
                    with patch.object(
                        TestFilter, "process", return_value=empty_df
                    ) as p4:
                        orchestrator = TestOrchestrator()
                        orchestrator.execute()

                        # EhJsonToDeltaExtractor should be called once
                        p1.assert_called_once()
                        # SimpleLoader should be called once
                        p2.assert_called_once()
                        # The transformer should be called once
                        p3.assert_called_once()
                        # The test transformer should be called once
                        p4.assert_called_once()
