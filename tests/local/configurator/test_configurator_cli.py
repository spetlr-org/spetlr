import sys
import unittest
from tempfile import NamedTemporaryFile
from textwrap import dedent

from atc import Configurator

from . import tables1


class TestConfigurator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()

    def test_01_import_config(self):
        c = Configurator()
        c.add_resource_path(tables1)

        name = "configurator_cli_tmp_file"

        with NamedTemporaryFile() as nf:
            name = nf.name
            nf.close()
            sys.argv = ["mycliprog", "generate-keys-file", "-o", name]
            with self.assertRaises(SystemExit) as ex:
                c.cli()
                self.assertEqual(ex.exception.code, 0)

            conts = open(name).read()
            expected = dedent(
                """\
                # AUTO GENERATED FILE.
                # contains all atc.Configurator keys

                ID = "ID"
                MNT = "MNT"
                MySecond = "MySecond"
                MyFirst = "MyFirst"
                MyAlias = "MyAlias"
                MyForked = "MyForked"
                MyRecursing = "MyRecursing"
            """
            )
            self.assertEqual(conts, expected)

            with open(name, "w") as f:
                f.write("garbage")

            # repeat the test
            sys.argv = ["mycliprog", "generate-keys-file", "-o", name]
            with self.assertRaises(SystemExit) as ex:
                c.cli()
                # This time the error code is set to 1 since the file was updated
                self.assertEqual(ex.exception.code, 1)

            # repeat the test
            sys.argv = ["mycliprog", "generate-keys-file", "-o", name]
            with self.assertRaises(SystemExit) as ex:
                c.cli()
                # This time the error code is set to 0 since the file contetns were ok
                self.assertEqual(ex.exception.code, 0)
