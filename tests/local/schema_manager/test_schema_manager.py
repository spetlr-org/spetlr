import unittest

import pyspark.sql.types as T

from spetlr.configurator import Configurator
from spetlr.exceptions import NoSuchSchemaException
from spetlr.schema_manager import SchemaManager
from spetlr.schema_manager.spark_schema import get_schema
from tests.local.schema_manager import extras
from tests.local.schema_manager.extras import initSchemaManager


class TestSchemaManager(unittest.TestCase):
    sc: SchemaManager

    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        SchemaManager().clear_all_configurations()
        cls.sc = initSchemaManager()

    def test_01_register_schema(self):
        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )

        self.assertNotIn("register_test", self.sc._registered_schemas)

        self.sc.register_schema(schema_name="register_test", schema=schema)

        self.assertIn("register_test", self.sc._registered_schemas)

    def test_02_get_registered_schema(self):
        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )

        self.assertNotIn("register_test2", self.sc._registered_schemas)

        self.sc.register_schema(schema_name="register_test2", schema=schema)

        self.assertEqual(
            schema,
            self.sc.get_schema(schema_identifier="register_test2"),
        )

    def test_03_get_schema_missing_exception(self):
        with self.assertRaises(NoSuchSchemaException):
            self.sc.get_schema(schema_identifier="TestTableNoSchema")

    def test_04_get_schema_missing_default_value(self):
        self.assertIsNone(
            self.sc.get_schema(
                schema_identifier="TestTableNoSchema",
                default=None,
            )
        )

    def test_05_get_python_ref_schema(self):
        schema = SchemaManager().get_schema(schema_identifier="TestTableSchema")

        expected_schema = extras.python_test_schema

        self.assertEqual(expected_schema, schema)

    def test_06_get_sql_schema(self):
        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )
        self.assertEqual(
            "Column1 int, Column2 string, Column3 float", self.sc.struct_to_sql(schema)
        )
        self.assertEqual(
            "Column1 int,\n  Column2 string,\n  Column3 float",
            self.sc.struct_to_sql(schema, formatted=True),
        )

    def test_07_parse_comments(self):
        d_field = get_schema(
            """
            d string COMMENT 'Whatsupp with "you"',
        """
        ).fields[0]
        self.assertEqual(d_field.metadata, {"comment": 'Whatsupp with "you"'})
