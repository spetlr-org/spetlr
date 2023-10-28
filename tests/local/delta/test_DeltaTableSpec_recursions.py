import unittest
from textwrap import dedent

from spetlr import Configurator
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec


class TestDeltaTableSpecRecursions(unittest.TestCase):
    """This class collects test cases for scenarios that
    have been known to cause problems"""

    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_prod()

    # not working yet.
    @unittest.skip
    def test_01_create_statements(self):
        _ = DeltaTableSpec.from_sql(
            dedent(
                """
            CREATE OR REPLACE TABLE test.mytbl
            (
                id BIGINT GENERATED ALWAYS AS IDENTITY,
                payload string
            )
            USING DELTA;
        """
            )
        )
