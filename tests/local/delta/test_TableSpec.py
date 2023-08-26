import unittest

from pyspark.sql import types as t

from spetlr import Configurator
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec


class TestDeltaTableSpec(unittest.TestCase):
    def test_01_check_init_protections(self):
        tbl1 = DeltaTableSpec(
            name="myDeltaTableSpecTestDb{ID}.table",
            schema=t.StructType(
                fields=[
                    t.StructField(name="c", dataType=t.DoubleType()),
                    t.StructField(
                        name="d",
                        dataType=t.StringType(),
                        nullable=False,
                        metadata={"comment": "Whatsupp"},
                    ),
                    t.StructField(name="onlyb", dataType=t.IntegerType()),
                    t.StructField(name="a", dataType=t.IntegerType()),
                    t.StructField(name="b", dataType=t.StringType()),
                ]
            ),
            location="/somewhere/over/the{ID}/rainbow",
        )
        tbl2 = DeltaTableSpec(
            name="mydeltatablespectestdb.table",
            schema=t.StructType(
                fields=[
                    t.StructField(name="c", dataType=t.DoubleType()),
                    t.StructField(
                        name="d",
                        dataType=t.StringType(),
                        nullable=False,
                        metadata={"comment": "Whatsupp"},
                    ),
                    t.StructField(name="onlyb", dataType=t.IntegerType()),
                    t.StructField(name="a", dataType=t.IntegerType()),
                    t.StructField(name="b", dataType=t.StringType()),
                ]
            ),
            location="dbfs:/somewhere/over/the/rainbow",
        )
        Configurator().set_prod()
        self.assertEqual(tbl1.fully_substituted(), tbl2)
