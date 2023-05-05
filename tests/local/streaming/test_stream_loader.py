from spetlrtools.testing import DataframeTestCase

from spetlr.etl.loaders.stream_loader import StreamLoader


class StreamLoaderTests(DataframeTestCase):
    def test_01(self):
        _loader = StreamLoader()

        ## Mock tests
