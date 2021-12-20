import unittest
from unittest.mock import MagicMock

from atc.etl import Orchestration, Orchestrator, Transformer, MultiInputTransformer


class MultipleExtractorOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.extractor.get_extractors():
            e.read.assert_called_once()

    def test_execute_invokes_transformer_process_many(self):
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
                .transform_with(MyMultiInputTransformer())
                .load_into(MagicMock())
                .build()
                )


class MultipleExtractorWithMultiLoaderOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.extractor.get_extractors():
            e.read.assert_called_once()

    def test_execute_invokes_inner_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        for i, transformer in enumerate(sut.transformer.get_transformers()):
            if i == 0:
                transformer.process_many.assert_called_once()
            else:
                transformer.process.assert_called_once()

    def test_execute_invokes_loader_save(self):
        sut = self._create_sut()
        sut.execute()
        sut.loader.save.assert_called_once()

    @staticmethod
    def _create_sut() -> Orchestrator:
        class MyMultiInputTransformer(MultiInputTransformer):
            process_many = MagicMock()
        
        class MyTransformer(Transformer):
            process = MagicMock()
        
        return (Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MyMultiInputTransformer())
                .transform_with(MyTransformer())
                .load_into(MagicMock())
                .build()
                )


class MultipleExtractorWithMultiLoaderOrchestratorTests(unittest.TestCase):

    def test_execute_invokes_inner_extractors(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.extractor.get_extractors():
            e.read.assert_called_once()

    def test_execute_invokes_transformer_process(self):
        sut = self._create_sut()
        sut.execute()
        sut.transformer.process_many.assert_called_once()

    def test_execute_invokes_inner_loaders(self):
        sut = self._create_sut()
        sut.execute()
        for e in sut.loader.get_loaders():
            e.save.assert_called_once()

    @staticmethod
    def _create_sut() -> Orchestrator:
        class MyMultiInputTransformer(MultiInputTransformer):
            process_many = MagicMock()
        
        return (Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MyMultiInputTransformer())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
                )

if __name__ == "__main__":
    unittest.main()