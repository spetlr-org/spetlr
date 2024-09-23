from datetime import datetime
from unittest.mock import Mock

from spetlrtools.testing import DataframeTestCase

from spetlr.delta import DeltaHandle


class DeltaHandleStreamTests(DataframeTestCase):
    def test_01_stream_starttime_datetime(self):
        DeltaHandle("test", stream_start=datetime(2020, month=1, day=1)).append(Mock())

    def test_02_stream_starttime_string(self):
        DeltaHandle("test", stream_start="2020-01-01").append(Mock())
