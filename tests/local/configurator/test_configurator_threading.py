import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from spetlr import Configurator
from tests.local.configurator import tables1


class TestConfigurator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

    def test_01_import_config(self):
        tc = Configurator()
        tc.add_resource_path(tables1)

        tc.reset(debug=True)
        self.assertRegex(tc.table_name("MyFirst"), "first__.*")
        self.assertRegex(tc.table_name("MyAlias"), "first__.*")
        self.assertRegex(tc.table_name("MyForked"), "another")

        tc.reset(debug=False)
        self.assertRegex(tc.table_name("MyFirst"), "first")
        self.assertRegex(tc.table_name("MyAlias"), "first")
        self.assertRegex(tc.table_name("MyForked"), "first")

    def test_02_threading(self):
        tc = Configurator()
        tc.set_prod()

        def register_and_check(index):
            thread = threading.current_thread()
            unique = f"{index}"
            tc.register(thread.name, unique)
            assert tc.get(thread.name) == unique

        with ThreadPoolExecutor() as executor:
            executor.map(register_and_check, range(100_000))

        # initial registrations still work
        self.assertRegex(tc.table_name("MyFirst"), "first")
