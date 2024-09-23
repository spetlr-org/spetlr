import unittest

from pyspark.sql import types as t

import spetlr.spark
from spetlr.etl.transformers.fuzzy_select import FuzzySelectTransformer


class FuzzySelectorTest(unittest.TestCase):
    def test_fuzzy1(self):
        target_columns = [
            "unChanged",
            "upperCase",
            "idStringId",
            "idStringABCProductId",
            "misSpelled",
        ]

        in_columns = [
            "unChanged",
            "UpperCase",
            "IDStringID",
            "IDStringABCProductId",  # this column is similar enough to the one above
            # that the standard match cutoff of 0.6 fails to associate it uniquely.
            "miisSpolled",
        ]

        res = dict(zip(in_columns, target_columns))

        self.assertRaises(
            spetlr.etl.transformers.fuzzy_select.NonUniqueException,
            FuzzySelectTransformer(target_columns).find_best_mapping,
            in_columns,
        )

        self.assertEqual(
            FuzzySelectTransformer(
                target_columns,
                # a tighter similarity constraint allows a unique association
                match_cutoff=0.8,
            ).find_best_mapping(
                in_columns,
            ),
            res,
        )

    def test_transform(self):
        ft = FuzzySelectTransformer(
            [
                "Index",
                "Count",
                "Label",
            ]
        )

        result = ft.process(
            spetlr.spark.Spark.get().createDataFrame(
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
