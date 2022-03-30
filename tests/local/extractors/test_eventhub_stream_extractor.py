import unittest
from datetime import datetime

from atc.extractors import (
    EventhubStreamExtractor,
    InvalidEventhubStreamExtractorParameters,
)


class EventhubStreamExtractorTest(unittest.TestCase):
    def test_create_with_connectionString(self):
        eventhubStreamExtractor = EventhubStreamExtractor(
            connectionString="testConnectionString",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger=100000,
        )

        self.assertEqual(
            eventhubStreamExtractor.connectionString, "testConnectionString"
        )

    def test_create_without_connectionString(self):
        eventhubStreamExtractor = EventhubStreamExtractor(
            namespace="testNamespace",
            eventhub="testEventhub",
            accessKeyName="testAccessKeyName",
            accessKey="testAccessKey",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger=100000,
        )

        self.assertEqual(
            eventhubStreamExtractor.connectionString,
            "Endpoint=sb://testNamespace.servicebus.windows.net/testEventhub;"
            "EntityPath=testEventhub;SharedAccessKeyName=testAccessKeyName;"
            "SharedAccessKey=testAccessKey",
        )

    def test_raise_create_exeption(self):
        with self.assertRaises(InvalidEventhubStreamExtractorParameters):
            EventhubStreamExtractor(
                connectionString=None,
                namespace=None,
                eventhub="testEventhub",
                accessKeyName="testAccessKeyName",
                accessKey="testAccessKey",
                consumerGroup="testConsumerGroup",
                maxEventsPerTrigger=100000,
            )

        with self.assertRaises(InvalidEventhubStreamExtractorParameters):
            EventhubStreamExtractor(
                connectionString=None,
                namespace="testNamespace",
                eventhub=None,
                accessKeyName="testAccessKeyName",
                accessKey="testAccessKey",
                consumerGroup="testConsumerGroup",
                maxEventsPerTrigger=100000,
            )

        with self.assertRaises(InvalidEventhubStreamExtractorParameters):
            EventhubStreamExtractor(
                connectionString=None,
                namespace="testNamespace",
                eventhub="testEventhub",
                accessKeyName=None,
                accessKey="testAccessKey",
                consumerGroup="testConsumerGroup",
                maxEventsPerTrigger=100000,
            )

        with self.assertRaises(InvalidEventhubStreamExtractorParameters):
            EventhubStreamExtractor(
                connectionString=None,
                namespace="testNamespace",
                eventhub="testEventhub",
                accessKeyName="testAccessKeyName",
                accessKey=None,
                consumerGroup="testConsumerGroup",
                maxEventsPerTrigger=100000,
            )

    def test_create_start_from_beginning_of_stream(self):
        eventhubStreamExtractor = EventhubStreamExtractor(
            connectionString="testConnectionString",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger=100000,
        )

        expectedStartingEventPosition = {
            "offset": "-1",  # Start stream from beginning
            "seqNo": -1,  # not in use
            "enqueuedTime": None,  # not in use
            "isInclusive": True,
        }

        self.assertEqual(
            eventhubStreamExtractor.startingEventPosition, expectedStartingEventPosition
        )

    def test_create_start_from_timestamp(self):
        timestampToUse = datetime.utcnow()

        eventhubStreamExtractor = EventhubStreamExtractor(
            connectionString="testConnectionString",
            consumerGroup="testConsumerGroup",
            maxEventsPerTrigger=100000,
            startEnqueuedTime=timestampToUse,
        )

        expectedStartingEventPosition = {
            "offset": None,  # not in use
            "seqNo": -1,  # not in use
            "enqueuedTime": timestampToUse.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),  # Start from timestamp
            "isInclusive": True,
        }

        self.assertEqual(
            eventhubStreamExtractor.startingEventPosition, expectedStartingEventPosition
        )

    def test_read_eventhub_stream(self):
        # Streaming is not testable at the moment
        pass


if __name__ == "__main__":
    unittest.main()
