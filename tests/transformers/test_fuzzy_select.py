import unittest

import atc.spark
from atc.transformers.fuzzy_select import FuzzySelectTransformer
from pyspark.sql import types as t


class FuzzySelectorTest(unittest.TestCase):
    def test_fuzzy1(self):
        ft = FuzzySelectTransformer(
            [
                "unChanged",
                "upperCase",
                "idStringId",
                "idStringABCProductId",
                "misSpelled",
            ]
        )

        d = ft.find_best_mapping(
            [
                "unChanged",
                "UpperCase",
                "IDStringID",
                "IDStringABCProductId",
                "miisSpolled",
            ]
        )

        res = {
            "unChanged": "unChanged",
            "UpperCase": "upperCase",
            "IDStringID": "idStringId",
            "IDStringABCProductId": "idStringABCProductId",
            "miisSpolled": "misSpelled",
        }
        self.assertEqual(d, res)

    def test_transform(self):

        ft = FuzzySelectTransformer(
            [
                "Index",
                "Count",
                "Label",
            ]
        )

        result = ft.process(
            atc.spark.Spark.get().createDataFrame(
                [(1, 2, "foo"), (3, 4, "bar")],
                t.StructType(
                    [
                        t.StructField("inex", t.IntegerType()),
                        t.StructField("count", t.IntegerType()),
                        t.StructField("lables", t.StringType()),
                    ]
                ),
            )
        )

        self.assertEqual(
            [
                "Index",
                "Count",
                "Label",
            ],
            result.columns,
        )
