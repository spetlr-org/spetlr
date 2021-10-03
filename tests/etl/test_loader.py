import unittest
from unittest.mock import MagicMock

from atc.etl import DelegatingLoader, Loader


class LoaderTests(unittest.TestCase):

    def test_process_returns_not_none(self):
        self.assertIsNotNone(Loader().save(MagicMock()))


class DelegatingLoaderTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sut = sut = DelegatingLoader([MagicMock(), MagicMock(), MagicMock()])
        cls.df = sut.save(MagicMock())

    def test_get_loaders_returns_not_none(self):
        sut = DelegatingLoader([MagicMock(), MagicMock(), MagicMock()])
        sut.save(MagicMock())
        self.assertIsNotNone(sut.get_loaders())

    def test_save_returns_not_none(self):
        sut = DelegatingLoader([MagicMock(), MagicMock(), MagicMock()])
        self.assertIsNotNone(sut.save(MagicMock()))

    def test_save_returns_dataframe(self):
        sut = DelegatingLoader([MagicMock(), MagicMock(), MagicMock()])
        sut.save(MagicMock())
        for e in self.sut.inner_loaders:
            e.save.assert_called_once()
