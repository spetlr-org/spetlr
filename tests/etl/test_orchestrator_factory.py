import unittest
from unittest.mock import MagicMock

from atc.etl import OrchestratorFactory


class OrchestratorFactoryTests(unittest.TestCase):

    def test_create_returns_not_none(self):
        self.assertIsNotNone(OrchestratorFactory.create(MagicMock(), MagicMock(), MagicMock()))

    def test_create_for_multiple_sources_returns_not_none(self):
        sut = OrchestratorFactory.create_for_multiple_sources(MagicMock(), MagicMock(), MagicMock())
        self.assertIsNotNone(sut)

    def test_create_for_multiple_transformers_returns_not_none(self):
        sut = OrchestratorFactory.create_for_multiple_transformers(MagicMock(), MagicMock(), MagicMock())
        self.assertIsNotNone(sut)

    def test_create_for_raw_ingestion_returns_not_none(self):
        sut = OrchestratorFactory.create_for_raw_ingestion(MagicMock(), MagicMock())
        self.assertIsNotNone(sut)
