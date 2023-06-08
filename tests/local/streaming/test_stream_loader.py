from unittest.mock import Mock

from spetlrtools.testing import DataframeTestCase

from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.exceptions import (
    NeedTriggerTimeWhenProcessingType,
    NotAValidStreamTriggerType,
    UnknownStreamOutputMode,
)


class StreamLoaderTests(DataframeTestCase):
    def test_01_wrong_triggertype(self):
        with self.assertRaises(NotAValidStreamTriggerType):
            StreamLoader(
                loader=Mock(),
                checkpoint_path="testpath",
                trigger_type="unknown",
            ).save(Mock())

    def test_02_processingtime(self):
        with self.assertRaises(NeedTriggerTimeWhenProcessingType):
            StreamLoader(
                loader=Mock(),
                checkpoint_path="testpath",
                trigger_type="processingtime",
            ).save(Mock())

    def test_03_wrong_output_mode(self):
        with self.assertRaises(UnknownStreamOutputMode):
            StreamLoader(
                loader=Mock(),
                outputmode="unknown",
                checkpoint_path="testpath",
                trigger_type="once",
            ).save(Mock())
