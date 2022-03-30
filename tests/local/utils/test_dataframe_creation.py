import unittest

from pyspark.sql import types

from atc.spark import Spark
from atc.utils import DataframeCreator


class DataframeCreatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Spark.get()

        cls.schema = types._parse_datatype_string(
            """
            Id INTEGER,
            measured DOUBLE,
            customer STRUCT<
                name:STRING,
                address:STRING
            >,
            product_nos ARRAY<STRUCT<
                no:INTEGER,
                name:STRING
            >>
        """
        )

    def test_full_creation(self):
        df = DataframeCreator.make(
            self.schema, [(1, 3.5, ("otto", "neverland"), [(1, "book")])]
        )
        df.show()
        rows = [row.asDict(True) for row in df.collect()]
        self.assertEqual(
            [
                dict(
                    Id=1,
                    measured=3.5,
                    customer=dict(name="otto", address="neverland"),
                    product_nos=[dict(no=1, name="book")],
                )
            ],
            rows,
        )

    def test_partial_creation(self):
        df = DataframeCreator.make_partial(
            schema=self.schema,
            columns=["Id", ("customer", ["name"]), ("product_nos", ["no"])],
            data=[
                (1, ("otto",), [(1,), (2,)]),
                (2, ("max",), []),
            ],
        )
        df.show()
        rows = [row.asDict(True) for row in df.collect()]
        self.assertEqual(
            [
                dict(
                    Id=1,
                    measured=None,
                    customer=dict(name="otto", address=None),
                    product_nos=[dict(no=1, name=None), dict(no=2, name=None)],
                ),
                dict(
                    Id=2,
                    measured=None,
                    customer=dict(name="max", address=None),
                    product_nos=[],
                ),
            ],
            rows,
        )
