import unittest

from spetlr import Configurator


class TestConfigurator(unittest.TestCase):

    def test_01_recursion(self):
        tc = Configurator()
        tc.clear_all_configurations()
        tc.register("BASE", "foobar")
        tc.register("FIRST", "really {BASE}")
        print(tc.get("FIRST"))

        tc.register("SECOND", " {BASE} really {FIRST}")
        print(tc.get("SECOND"))
