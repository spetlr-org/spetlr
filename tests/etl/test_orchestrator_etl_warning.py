import contextlib
import io
import subprocess
import sys
import unittest
from pathlib import Path

from pyspark.sql import DataFrame

from atc.etl import Extractor, Orchestrator


class OrchestratorEtlTests(unittest.TestCase):
    def test_no_special_case(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__()
                self.extract_from(MyEx())

        with io.StringIO() as buf:
            # run the tests
            with contextlib.redirect_stdout(buf):
                _ = MyO().execute()
            self.assertEqual("", buf.getvalue())

    def test_misuse_warning(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__()
                self.extract_from(MyEx())

        with io.StringIO() as buf:
            # run the tests
            with contextlib.redirect_stdout(buf):
                _ = MyO().execute({"input": "foobar"})
            self.assertRegex(buf.getvalue(), "WARNING: You used.*")

    def test_misuse_warning_suppressed(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__(suppress_composition_warning=True)
                self.extract_from(MyEx())

        with io.StringIO() as buf:
            # run the tests
            with contextlib.redirect_stdout(buf):
                _ = MyO().execute({"input": "foobar"})
            #     No warning. Suppressed.
            self.assertEqual("", buf.getvalue())

    def test_correct_use_no_warning(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                if len(self.previous_extractions):
                    return self.previous_extractions.popitem()[1]
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__()
                self.extract_from(MyEx())

        with io.StringIO() as buf:
            # run the tests
            with contextlib.redirect_stdout(buf):
                _ = MyO().execute({"input": "foobar"})
            #     No warning. extractor handled inputs.
            self.assertEqual("", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
