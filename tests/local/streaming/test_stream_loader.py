from unittest.mock import Mock

from spetlrtools.testing import DataframeTestCase

from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.exceptions import (
    AmbiguousLoaderInput,
    MissingEitherStreamLoaderOrHandle,
    NeedTriggerTimeWhenProcessingType,
    NotAValidStreamTriggerType,
    StreamLoaderNeedsFormatAndCheckpoint,
    UnknownStreamOutputMode,
)


class StreamLoaderTests(DataframeTestCase):
    def test_01_wrong_triggertype(self):
        with self.assertRaises(NotAValidStreamTriggerType):
            StreamLoader(
                handle=Mock(),
                options_dict={},
                format="delta",
                await_termination=True,
                mode="append",
                checkpoint_path="testpath",
                trigger_type="unknown",
            ).save(Mock())

    def test_02_processingtime(self):
        with self.assertRaises(NeedTriggerTimeWhenProcessingType):
            StreamLoader(
                handle=Mock(),
                options_dict={},
                format="delta",
                await_termination=True,
                mode="append",
                checkpoint_path="testpath",
                trigger_type="processingtime",
            ).save(Mock())

    def test_03_no_handle_or_loader(self):
        with self.assertRaises(MissingEitherStreamLoaderOrHandle):
            StreamLoader(
                options_dict={},
                format="delta",
                await_termination=True,
                mode="append",
                checkpoint_path="testpath",
                trigger_type="unknown",
            ).save(Mock())

    def test_04_ambiguous_loader(self):
        with self.assertRaises(AmbiguousLoaderInput):
            StreamLoader(
                handle=Mock(),
                loader=Mock(),
                options_dict={},
                format="delta",
                await_termination=True,
                mode="append",
                checkpoint_path="testpath",
                trigger_type="unknown",
            ).save(Mock())

    def test_05_wrong_output(self):
        with self.assertRaises(UnknownStreamOutputMode):
            StreamLoader(
                handle=Mock(),
                options_dict={},
                format="delta",
                await_termination=True,
                outputmode="unknown",
                checkpoint_path="testpath",
                trigger_type="once",
            ).save(Mock())

    def test_06_loader_needs_format_and_checkpoint(self):
        with self.assertRaises(StreamLoaderNeedsFormatAndCheckpoint):
            StreamLoader(
                loader=Mock(),
                options_dict={},
                await_termination=True,
                outputmode="unknown",
                checkpoint_path="testpath",
                trigger_type="once",
            ).save(Mock())

        with self.assertRaises(StreamLoaderNeedsFormatAndCheckpoint):
            StreamLoader(
                loader=Mock(),
                options_dict={},
                await_termination=True,
                format="something",
                outputmode="unknown",
                trigger_type="once",
            ).save(Mock())

        with self.assertRaises(StreamLoaderNeedsFormatAndCheckpoint):
            StreamLoader(
                loader=Mock(),
                options_dict={},
                await_termination=True,
                outputmode="unknown",
                trigger_type="once",
            ).save(Mock())

    def test_07_discprency_mode_outputmode(self):
        """
        Here should be a test of mode and outputmode!

        """
        pass
