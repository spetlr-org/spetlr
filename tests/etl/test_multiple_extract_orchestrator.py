import unittest
from unittest.mock import MagicMock

from atc.etl import Orchestration, Orchestrator, MultiInputTransformer


class MultipleExtractOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.extractor.get_extractors():
            e.read.assert_called_once()

    def test_execute_invokes_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        sut.transformer.process_many.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut() -> Orchestrator:
        class MyMultiInputTransformer(MultiInputTransformer):
            process_many = MagicMock()

        return (Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MyMultiInputTransformer())
                .load_into(MagicMock())
                .build()
                )
