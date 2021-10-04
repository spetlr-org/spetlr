import unittest
from unittest.mock import MagicMock

from atc.etl import Loader, Orchestration


class LoaderTests(unittest.TestCase):

    def test_process_returns_not_none(self):
        self.assertIsNotNone(Loader().save(MagicMock()))


class DelegatingLoaderTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.sut = sut = cls.create_sut()
        cls.df = sut.save(MagicMock())

    @staticmethod
    def create_sut():
        orch = (Orchestration
                .extract_from(MagicMock())
                .transform_with(MagicMock())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
                )
        return orch.loader

    def test_get_loaders_returns_not_none(self):
        self.sut.save(MagicMock())
        self.assertIsNotNone(self.sut.get_loaders())

    def test_save_returns_not_none(self):
        self.assertIsNotNone(self.sut.save(MagicMock()))

    def test_save_returns_dataframe(self):
        sut = self.create_sut()
        sut.save(MagicMock())
        for e in sut.inner_loaders:
            e.save.assert_called_once()
