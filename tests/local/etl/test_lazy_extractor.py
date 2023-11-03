import unittest

from spetlrtools.testing import TestHandle

from spetlr.etl import Orchestrator
from spetlr.etl.extractors import LazySimpleExtractor


class TestLazyExtractor(unittest.TestCase):
    def test_lazy_extractor(self):
        # This is the orchestrator. It takes any lunch it is given
        # and returns it. If none is given, it extracts bacon and
        # returns it.
        o = Orchestrator()
        o.extract_from(LazySimpleExtractor(TestHandle(provides="bacon"), "lunch"))

        # Given spam? return spam
        lazy_result = o.execute({"lunch": "spam"})
        self.assertEqual(lazy_result["lunch"], "spam")

        # no lunch? use bacon
        active_result = o.execute()
        self.assertEqual(active_result["lunch"], "bacon")
