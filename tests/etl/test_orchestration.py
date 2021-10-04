import unittest
from typing import Dict
from unittest.mock import MagicMock

from pyspark.sql import DataFrame

from atc.etl import Orchestration, MultiInputTransformer


class OrchestrationTests(unittest.TestCase):

    def test_create_returns_not_none(self):
        sut = (Orchestration
               .extract_from(MagicMock())
               .transform_with(MagicMock())
               .load_into(MagicMock())
               .build())
        self.assertIsNotNone(sut)
        self.assertEqual("SingleOrchestrator", type(sut).__name__)

    def test_create_for_multiple_sources_returns_not_none(self):
        class MyMultiInputTransformer(MultiInputTransformer):
            def process_many(self, dataset: Dict[str, DataFrame]) -> DataFrame:
                pass

        sut = (Orchestration
               .extract_from(MagicMock())
               .extract_from(MagicMock())
               .extract_from(MagicMock())
               .extract_from(MagicMock())
               .transform_with(MyMultiInputTransformer())
               .load_into(MagicMock())
               .build())
        self.assertIsNotNone(sut

                             )
        self.assertEqual("MultipleExtractOrchestrator", type(sut).__name__)

    def test_create_for_multiple_transformers_returns_not_none(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .transform_with(MagicMock())
                .transform_with(MagicMock())
                .transform_with(MagicMock())
                .load_into(MagicMock)
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("MultipleTransformOrchestrator", type(sut).__name__)

    def test_create_for_raw_ingestion_returns_not_none(self):
        sut = (
            Orchestration
                .extract_from(MagicMock())
                .load_into(MagicMock())
                .build()
        )
        self.assertIsNotNone(sut)
        self.assertEqual("NoTransformOrchestrator", type(sut).__name__)

    def test_create_for_multiple_destinations_returns_not_none(self):
        sut = (Orchestration
               .extract_from(MagicMock())
               .transform_with(MagicMock())
               .load_into(MagicMock())
               .load_into(MagicMock())
               .build())
        self.assertIsNotNone(sut)
        self.assertEqual("MultipleLoaderOrchestrator", type(sut).__name__)
