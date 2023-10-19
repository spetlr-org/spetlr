import unittest

from spetlrtools.testing import TestHandle

from spetlr.etl import Orchestrator
from spetlr.etl.extractors import LazySimpleExtractor


class TestLazyExtractor(unittest.TestCase):
    def test_lazy_extractor(self):
        handle = TestHandle(provides="bacon")
        o = Orchestrator()
        o.extract_from(LazySimpleExtractor(handle, "lunch"))

        lazy_result = o.execute({"lunch": "spam"})
        self.assertEqual(lazy_result["lunch"], "spam")

        active_result = o.execute()
        self.assertEqual(active_result["lunch"], "bacon")
