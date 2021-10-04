import unittest
from unittest.mock import MagicMock

from atc.etl import Orchestration, Orchestrator


class MultipleExtractOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        sut.extractor.read.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut()->Orchestrator:
        return (Orchestration
                .extract_from(MagicMock())
                .load_into(MagicMock())
                .build()
                )
