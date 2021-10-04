import unittest
from unittest.mock import MagicMock

from atc.etl import Orchestration, Orchestrator


class MultipleTransformOrchestratorTests(unittest.TestCase):

    def test_execute_extractor_read(self):
        sut = self._create_sut_with_mocks()
        sut.execute()
        sut.extractor.read.assert_called_once()

    def test_inner_transformer_is_delegating_transformer(self):
        sut = self._create_sut_with_mocks()
        self.assertEqual("DelegatingTransformer",sut.transformer.__class__.__name__)

    def test_execute_invokes_inner_transformer_process(self):
        sut = self._create_sut_with_mocks()
        sut.execute()
        for transformer in sut.transformer.get_transformers():
            transformer.process.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut_with_mocks()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut_with_mocks() -> Orchestrator:
        return (Orchestration
                .extract_from(MagicMock())
                .transform_with(MagicMock())
                .transform_with(MagicMock())
                .transform_with(MagicMock())
                .load_into(MagicMock())
                .build()
                )
