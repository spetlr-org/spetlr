import unittest
from unittest.mock import MagicMock

from atc import Orchestrator, OrchestratorFactory, DelegatingExtractor


class MultipleExtractOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.extractor.get_extractors():
            e.read.assert_called_once()

    def test_execute_invokes_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        sut.transformer.process.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut() -> Orchestrator:
        return OrchestratorFactory.create_for_multiple_sources(DelegatingExtractor([MagicMock(),
                                                                                    MagicMock(),
                                                                                    MagicMock()]),
                                                               MagicMock(),
                                                               MagicMock())
