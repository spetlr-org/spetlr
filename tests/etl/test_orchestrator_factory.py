import unittest
from unittest.mock import MagicMock

from atc import OrchestratorFactory


class OrchestratorFactoryTests(unittest.TestCase):

    def test_create_returns_not_none(self):
        self.assertIsNotNone(OrchestratorFactory.create(MagicMock(), MagicMock(), MagicMock()))

    def test_create_for_multiple_sources_returns_not_none(self):
        sut = OrchestratorFactory.create_for_multiple_sources(MagicMock(), MagicMock(), MagicMock())
        self.assertIsNotNone(sut)
