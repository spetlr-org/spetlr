import unittest

from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from spetlr.sqlrepr.sql_types import repr_sql_types


class TestReprSqlTypes(unittest.TestCase):
    def test_Primitive(self):
        self.assertEqual(repr_sql_types(StringType()), "StringType()")
        self.assertEqual(repr_sql_types(IntegerType()), "IntegerType()")
        self.assertEqual(repr_sql_types(FloatType()), "FloatType()")
        self.assertEqual(repr_sql_types(LongType()), "LongType()")

    def test_Decimal(self):
        self.assertEqual(
            repr_sql_types(DecimalType(precision=10, scale=2)),
            "DecimalType(precision=10, scale=2)",
        )

    def test_Array(self):
        self.assertEqual(
            repr_sql_types(ArrayType(elementType=IntegerType())),
            "ArrayType(elementType=IntegerType())",
        )

    def test_Map(self):
        self.assertEqual(
            repr_sql_types(MapType(keyType=IntegerType(), valueType=StringType())),
            "MapType(keyType=IntegerType(), "
            "valueType=StringType(), "
            "valueContainsNull=True)",
        )

    def test_StructField(self):
        self.assertEqual(
            repr_sql_types(
                StructField(
                    name="myfield",
                    dataType=IntegerType(),
                    nullable=False,
                    metadata={"foo": "bar"},
                )
            ),
            (
                "StructField(name='myfield', "
                "dataType=IntegerType(), "
                "nullable=False, "
                "metadata={'foo': 'bar'})"
            ),
        )

    def test_Struct(self):
        s = StructType(
            fields=[
                StructField(
                    name="myfield",
                    dataType=IntegerType(),
                    nullable=False,
                ),
                StructField(
                    name="b",
                    dataType=StringType(),
                    nullable=False,
                ),
            ]
        )
        self.assertEqual(
            repr_sql_types(s),
            (
                "StructType(fields=["
                "StructField(name='myfield', dataType=IntegerType(), "
                "nullable=False), "
                "StructField(name='b', dataType=StringType(), "
                "nullable=False)])"
            ),
        )
