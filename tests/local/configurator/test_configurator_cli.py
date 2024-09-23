import sys
import unittest
from tempfile import NamedTemporaryFile
from textwrap import dedent

from spetlr import Configurator
from spetlr.exceptions.cli_exceptions import SpetlrCliCheckFailed

from . import tables1


class TestConfiguratorCli(unittest.TestCase):
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
            sys.argv = ["mycliprog", "generate-keys-file", "-c", "-o", name]
            with self.assertRaises(SpetlrCliCheckFailed):
                # file did not exist. exit code 1
                c.cli()

            sys.argv = ["mycliprog", "generate-keys-file", "--output-file", name]
            c.cli()
            # file written. clean exit

            conts = open(name).read()
            expected = dedent(
                """\
                # AUTO GENERATED FILE
                # contains all spetlr.Configurator keys

                ID = "ID"
                MNT = "MNT"
                MyAlias = "MyAlias"
                MyFirst = "MyFirst"
                MyForked = "MyForked"
                MyRecursing = "MyRecursing"
                MySecond = "MySecond"
            """
            )
            self.assertEqual(conts, expected)

            # repeat the test
            sys.argv = ["mycliprog", "generate-keys-file", "--check-only", "-o", name]
            c.cli()
            # check passes since the file contents were ok

            with open(name, "w") as f:
                f.write(expected[:-10])  # bad contents

            sys.argv = ["mycliprog", "generate-keys-file", "-c", "-o", name]
            with self.assertRaises(SpetlrCliCheckFailed):
                # file had bad contents
                c.cli()
