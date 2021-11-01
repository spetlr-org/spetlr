import unittest

from atc.extractors import EventhubStreamExtractor, InvalidEventhubStreamExtractorParameters

class EventhubStreamExtractorTest(unittest.TestCase):

    def test_create_with_connectionString(self):
        eventhubStreamExtractor = EventhubStreamExtractor(
            connectionString="testConnectionString",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger = 100000
        )

        self.assertEqual(eventhubStreamExtractor.connectionString, "testConnectionString")

    def test_create_without_connectionString(self):
        eventhubStreamExtractor = EventhubStreamExtractor(
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testAccessKeyName",
            accessKey="testAccessKey",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger = 100000
        )

        self.assertEqual(eventhubStreamExtractor.connectionString, "Endpoint=sb://testNamespace.servicebus.windows.net/testEventhub;EntityPath=testEventhub;SharedAccessKeyName=testAccessKeyName;SharedAccessKey=testAccessKey")

    def test_raise_create_exeption(self):
        with self.assertRaises(InvalidEventhubStreamExtractorParameters) as context:
            EventhubStreamExtractor(
                consumerGroup="testConsumerGroup",
                maxEventsPerTrigger = 100000
            )

    def test_read_eventhub_stream(self):
        # Streaming is not testable at the moment
        pass

if __name__ == "__main__":
    unittest.main()