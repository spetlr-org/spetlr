import unittest
from unittest.mock import MagicMock

from atc.etl import Orchestration


class MultipleExtractOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_extractor_read(self):
        sut = self._create_sut()
        sut.execute()
        sut.extractor.read.assert_called_once()

    def test_execute_invokes_inner_loaders(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.loader.get_loaders():
            e.save.assert_called_once()

    def test_execute_invokes_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        sut.transformer.process.assert_called_once()

    @staticmethod
    def _create_sut():
        return (Orchestration
                .extract_from(MagicMock())
                .transform_with(MagicMock())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
                )
