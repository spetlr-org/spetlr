import unittest

import spetlr


class TestConfiguration:
    def __init__(self):
        tc = spetlr.Configurator()

        self.a = tc.define("MyTable", name="MyTable{ID}", path="/my/path{ID}/to/table")

        self.b = tc.define("MyAlias", alias="MyTable")
        self.c = tc.define(
            "MySplit", release={"alias": "MyTable"}, debug={"name": "SplitDebugLocal"}
        )


class ConfigObjectTests(unittest.TestCase):
    conf: TestConfiguration

    @classmethod
    def setUpClass(cls) -> None:
        spetlr.Configurator().clear_all_configurations()
        cls.conf = TestConfiguration()

    def test_a(self):
        tc = spetlr.Configurator()
        self.assertEqual(str(self.conf.a), "MyTable")

        tc.set_prod()
        self.assertEqual(self.conf.a.name, "MyTable")

        tc.set_debug()
        path: str = self.conf.a.path
        self.assertTrue(path.startswith("/my/path__"))
        self.assertTrue(path.endswith("/to/table"))

    def test_b(self):
        tc = spetlr.Configurator()
        tc.set_prod()
        self.assertEqual(self.conf.b.name, "MyTable")

    def test_c(self):
        tc = spetlr.Configurator()
        tc.set_prod()
        self.assertEqual(self.conf.c.name, "MyTable")

        tc.set_debug()
        self.assertEqual(self.conf.c.name, "SplitDebugLocal")
