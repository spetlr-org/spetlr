import unittest
from datetime import datetime as dt
from datetime import timezone

from atc.eh.EventHubCaptureExtractor import EventHubCaptureExtractor

utc = timezone.utc


class EventHubCaptureExtractorTests(unittest.TestCase):
    def test_breakup(self):
        eh = EventHubCaptureExtractor(
            path="/does/not/matter/since/unused", partitioning="ymdh"
        )
        parts = eh._break_into_partitioning_parts(
            dt(2020, 11, 30, 22, tzinfo=utc), dt(2022, 5, 3, 3, tzinfo=utc)
        )
        self.assertEqual(
            parts,
            [
                "y=2020/m=11/d=30/h=22/",
                "y=2020/m=11/d=30/h=23/",
                "y=2020/m=12/",
                "y=2021/",
                "y=2022/m=01/",
                "y=2022/m=02/",
                "y=2022/m=03/",
                "y=2022/m=04/",
                "y=2022/m=05/d=01/",
                "y=2022/m=05/d=02/",
                "y=2022/m=05/d=03/h=00/",
                "y=2022/m=05/d=03/h=01/",
                "y=2022/m=05/d=03/h=02/",
            ],
        )

    def test_open_interval(self):
        eh = EventHubCaptureExtractor(
            path="/does/not/matter/since/unused", partitioning="ymdh"
        )
        eh._now_utc = lambda: dt(2022, 5, 3, 3, tzinfo=utc)
        fp, tp = eh._validated_slice_arguments(dt(2020, 11, 30, 22, tzinfo=utc), None)

        self.assertEqual(tp, dt(2023, 1, 1, tzinfo=utc))

        parts = eh._break_into_partitioning_parts(fp, tp)
        self.assertEqual(
            parts,
            [
                "y=2020/m=11/d=30/h=22/",
                "y=2020/m=11/d=30/h=23/",
                "y=2020/m=12/",
                "y=2021/",
                "y=2022/",
            ],
        )
