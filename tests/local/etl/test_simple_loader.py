import unittest

from spetlrtools.testing import TestHandle

from spetlr.etl.loaders import SimpleLoader


class SimpleLoaderTests(unittest.TestCase):
    def test_pass_merge_overwrite(self):
        th = TestHandle()
        sl = SimpleLoader(th, overwriteSchema=True)
        sl.save("hello")
        self.assertEqual(th.overwritten, "hello")
        self.assertEqual(th.overwriteSchema, True)
