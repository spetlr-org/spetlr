import subprocess
import sys
import unittest
from pathlib import Path


class OrchestratorTests(unittest.TestCase):
    def test_examples(self):
        examples_folder = (
            Path(__file__).parent.parent.parent.parent / "examples" / "etl"
        )
        for file in examples_folder.iterdir():
            if file.suffix == ".py":
                subprocess.run([sys.executable, str(file)], check=True)


if __name__ == "__main__":
    unittest.main()
