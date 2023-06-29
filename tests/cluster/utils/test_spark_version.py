import unittest
from unittest.mock import Mock

from spetlrtools.testing import DataframeTestCase

from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.exceptions import SparkVersionNotSupportedForSpetlrStreaming
from spetlr.filehandle import FileHandle
from spetlr.spark import Spark


# Tests are runned for spark versions lower than 10_4
# To ensure that the correct assertions are made.
@unittest.skipUnless(
    Spark.version() < Spark.DATABRICKS_RUNTIME_10_4,
    f"Sparkversion tests only applies for spark runtime versions lower than 10_4,"
    f"Your version {Spark.version()}. Skipping...",
)
class SparkVersionTests(DataframeTestCase):
    def test_01_streamloader(self):
        with self.assertRaises(SparkVersionNotSupportedForSpetlrStreaming):
            StreamLoader(
                loader=Mock(),
                options_dict={},
                await_termination=True,
                checkpoint_path="/_testpath",
            ).save(Mock())

    def test_02_filehandle(self):
        with self.assertRaises(SparkVersionNotSupportedForSpetlrStreaming):
            FileHandle(
                file_location="test", schema_location="test", data_format="test"
            ).read_stream()
