import unittest
import warnings

from pyspark.sql import DataFrame

from atc.etl import Extractor, Orchestrator


class OrchestratorEtlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("error")

    def test_no_special_case(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__()
                self.extract_from(MyEx())

        warnings.filterwarnings("error")
        # no warning
        _ = MyO().execute()

    def test_misuse_warning(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__()
                self.extract_from(MyEx())

        with self.assertRaises(UserWarning):
            warnings.filterwarnings("error")
            _ = MyO().execute({"input": "foobar"})

    def test_misuse_warning_suppressed(self):
        class MyEx(Extractor):
            def read(self) -> DataFrame:
                return "hi"

        class MyO(Orchestrator):
            def __init__(self):
                super().__init__(suppress_composition_warning=True)
                self.extract_from(MyEx())

        warnings.filterwarnings("error")
        # no warning
        _ = MyO().execute()

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

        warnings.filterwarnings("error")
        # no warning
        _ = MyO().execute()


if __name__ == "__main__":
    unittest.main()
