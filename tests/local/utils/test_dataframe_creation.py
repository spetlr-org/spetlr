import unittest

from spetlr.schema_manager.spark_schema import get_schema
from spetlr.spark import Spark
from spetlr.utils import DataframeCreator


class DataframeCreatorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Spark.get()

        cls.schema = get_schema(
            """
            Id INTEGER,
            measured DOUBLE,
            customer STRUCT<
                name:STRING,
                address:STRING
            >,
            product_nos ARRAY<STRUCT<
                no:INTEGER,
                name:STRING,
                additional_data:MAP<STRING,STRING>
            >>
        """
        )

    def test_full_creation(self):
        df = DataframeCreator.make(
            self.schema,
            [(1, 3.5, ("otto", "neverland"), [(1, "book", {"foo": "bar"})])],
        )
        df.show()
        rows = [row.asDict(True) for row in df.collect()]
        self.assertEqual(
            [
                dict(
                    Id=1,
                    measured=3.5,
                    customer=dict(name="otto", address="neverland"),
                    product_nos=[
                        dict(no=1, name="book", additional_data=dict(foo="bar"))
                    ],
                )
            ],
            rows,
        )

    def test_partial_creation(self):
        df = DataframeCreator.make_partial(
            schema=self.schema,
            columns=[
                "Id",
                ("customer", ["name"]),
                ("product_nos", ["no", "additional_data"]),
            ],
            data=[
                (1, ("otto",), [(1, dict(foo="bar")), (2, dict(eggs="bacon"))]),
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
                    product_nos=[
                        dict(no=1, name=None, additional_data=dict(foo="bar")),
                        dict(no=2, name=None, additional_data=dict(eggs="bacon")),
                    ],
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
