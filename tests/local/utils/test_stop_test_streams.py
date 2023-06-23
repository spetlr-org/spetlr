from spetlrtools.testing import DataframeTestCase

from spetlr.exceptions import OnlyUseInSpetlrDebugMode
from spetlr.testutils.stop_test_streams import stop_test_streams


class TestStopTestStreams(DataframeTestCase):
    def test_01_configurator_must_be_debug(self):
        with self.assertRaises(OnlyUseInSpetlrDebugMode):
            stop_test_streams()
