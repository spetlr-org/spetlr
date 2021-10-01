import unittest
from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from atc.etl import OrchestratorFactory, MultipleTransformOrchestrator, DelegatingTransformer, Transformer


class MultipleTransformOrchestratorTests(unittest.TestCase):

    def test_execute_extractor_read(self):
        sut = self._create_sut_with_mocks()
        sut.execute()
        sut.extractor.read.assert_called_once()

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
    def _create_sut_with_mocks() -> MultipleTransformOrchestrator:
        return OrchestratorFactory.create_for_multiple_transformers(MagicMock(),
                                                                    DelegatingTransformer([MagicMock(), MagicMock(), MagicMock()]),
                                                                    MagicMock())


class DelegatingTransformerOrchestratorTests(unittest.TestCase):

    @staticmethod
    def _create_sut() -> MultipleTransformOrchestrator:
        return OrchestratorFactory.create_for_multiple_transformers(MagicMock(),
                                                                    DelegatingTransformer([RemoveEmptyValuesTransformer(),
                                                                                           DropDuplicatesTransformer(),
                                                                                           DateFormatTransformer()]),
                                                                    MagicMock())


class RemoveEmptyValuesTransformer(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        # remove empty values
        return df


class DropDuplicatesTransformer(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        # drop duplicates
        return df


class DateFormatTransformer(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        pass

    def process(self, df):
        # manipulate date formats
        return df
