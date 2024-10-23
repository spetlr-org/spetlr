"""
This is the default main file that is pushed to databricks to launch the test task.
Its tasks are
- to unpack the test archive,
- run the tests using pytest
This file is not intended to be used directly.
"""

import argparse
import os
import shutil
import sys
from tempfile import TemporaryDirectory

import pytest


def test_main():
    """Main function to be called inside the test job task. Do not use directly."""
    parser = argparse.ArgumentParser(description="Run Test Cases.")

    parser.add_argument("--archive")

    # relative path of test folder in test archive
    parser.add_argument("--folder")

    args = parser.parse_args()
    archive: str = args.archive
    folder: str = args.folder

    with TemporaryDirectory() as tmpdir:
        os.chdir(tmpdir)

        sys.path = [tmpdir] + sys.path

        # unzip test archive to base folder
        shutil.copy(archive, "tests.zip")
        shutil.unpack_archive("tests.zip")
        os.unlink("tests.zip")

        # Ensure Spark is initialized before any tests are run
        # the import statement is inside the function so that the outer file
        # can be imported even where pyspark may not be available
        from spetlr.spark import Spark

        Spark.get()

        retcode = pytest.main(["-x", folder])
        if retcode.value:
            raise Exception("Pytest failed")


if __name__ == "__main__":
    test_main()
