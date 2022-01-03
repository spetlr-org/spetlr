import unittest
from typing import Dict
from unittest.mock import MagicMock

from atc.etl.orchestrator import OrchestratorBuilderException, LogicError
from atc.etl import Orchestration, Transformer, MultiInputTransformer


class OrchestrationTests(unittest.TestCase):
    
    def test_create_no_transform_orchestrator(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("NoTransformOrchestrator", type(sut).__name__)

    def test_create_no_transform_orchestrator_with_multi_loader(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("NoTransformOrchestrator", type(sut).__name__)
    
    def test_create_single_extractor_orchestrator(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .transform_with(Transformer())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("SingleExtractorOrchestrator", type(sut).__name__)

    def test_create_single_extractor_orchestrator_with_multi_transformer(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .transform_with(Transformer())
                .transform_with(Transformer())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("SingleExtractorOrchestrator", type(sut).__name__)

    def test_create_single_extractor_orchestrator_with_multi_loader(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .transform_with(Transformer())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("SingleExtractorOrchestrator", type(sut).__name__)

    def test_create_multiple_extractor_orchestrator(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MultiInputTransformer())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("MultipleExtractorOrchestrator", type(sut).__name__)

    def test_create_multiple_extractor_orchestrator_with_multi_transformer(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MultiInputTransformer())
                .transform_with(Transformer())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("MultipleExtractorOrchestrator", type(sut).__name__)

    def test_create_multiple_extractor_orchestrator_with_multi_loader(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .extract_from(MagicMock())
                .transform_with(MultiInputTransformer())
                .load_into(MagicMock())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("MultipleExtractorOrchestrator", type(sut).__name__)

    def test_set_all_extractors_before_transformers_exception(self):
        with self.assertRaises(OrchestratorBuilderException) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .transform_with(Transformer())
                    .extract_from(MagicMock())
                    .load_into(MagicMock())
                    .build()
            )
    
    def test_set_all_extractors_before_loaders_exception(self):
        with self.assertRaises(OrchestratorBuilderException) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .transform_with(Transformer())
                    .load_into(MagicMock())
                    .extract_from(MagicMock())
                    .build()
            )

    def test_set_all_transformers_before_loaders_exception(self):
        with self.assertRaises(OrchestratorBuilderException) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .transform_with(Transformer())
                    .load_into(MagicMock())
                    .transform_with(Transformer())
                    .build()
            )

    def test_not_using_multi_input_transformer_when_having_multi_extractors_exception(self):
        with self.assertRaises(OrchestratorBuilderException) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .extract_from(MagicMock())
                    .transform_with(Transformer())
                    .load_into(MagicMock())
                    .build()
            )

    def test_using_multi_input_transformer_when_having_single_extractors_exception(self):
        with self.assertRaises(OrchestratorBuilderException) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .transform_with(MultiInputTransformer())
                    .load_into(MagicMock())
                    .build()
            )

    def test_no_loader_exception(self):
        with self.assertRaises(LogicError) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .transform_with(Transformer())
                    .build()
            )

    def test_multi_extractors_with_no_transformer_exception(self):
        with self.assertRaises(LogicError) as context:
            sut = (
                Orchestration
                    .extract_from(MagicMock())
                    .extract_from(MagicMock())
                    .load_into(MagicMock())
                    .build()
            )


if __name__ == "__main__":
    unittest.main()