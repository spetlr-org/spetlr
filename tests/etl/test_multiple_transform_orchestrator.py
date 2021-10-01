import unittest
from unittest.mock import MagicMock

from atc.etl import OrchestratorFactory, MultipleTransformOrchestrator, DelegatingTransformer


class MultipleTransformOrchestratorTests(unittest.TestCase):

    def test_execute_extractor_read(self):
        sut = self._create_sut()
        sut.execute()
        sut.extractor.read.assert_called_once()

    def test_execute_invokes_inner_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        for transformer in sut.transformer.get_transformers():
            transformer.process.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut() -> MultipleTransformOrchestrator:
        return OrchestratorFactory.create_for_multiple_transformers(MagicMock(),
                                                                    DelegatingTransformer([MagicMock(),MagicMock(),MagicMock()]),
                                                                    MagicMock())
