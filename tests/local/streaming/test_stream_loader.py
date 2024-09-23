import warnings
from unittest.mock import Mock

from spetlrtools.testing import DataframeTestCase

from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.exceptions import (
    InvalidStreamTriggerType,
    NeedTriggerTimeWhenProcessingType,
    UnknownStreamOutputMode,
)


class StreamLoaderTests(DataframeTestCase):
    def test_01_wrong_triggertype(self):
        with self.assertRaises(InvalidStreamTriggerType):
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

    def test_04_checkpoint_overwrite_warning(self):
        """
        Tests that if an checkpoint path is provided
        as checkpoint_path,
        the user is warned, that it will overwrite the
        pre-existing checkpointLocation in the options dict,
        if any found.
        """

        with self.assertRaises(UserWarning):
            warnings.filterwarnings("error")

            StreamLoader(
                loader=Mock(),
                checkpoint_path="testpath",
                trigger_type="once",
                options_dict={"checkpointLocation": "anothertestpath"},
            ).save(Mock())
