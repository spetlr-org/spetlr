import unittest

@unittest.skip("Current test pipeline does not support streaming yet.")
# This file should test the extractor "eventhub_stream_extractor" in atc/functions
class EventhubStreamExtractorTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
