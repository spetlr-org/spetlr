import unittest

import pyspark.sql.types as T

from atc.configurator import Configurator
from atc.schema_manager import SchemaManager

from . import extras
from .extras import initSchemaManager


class TestSchemaManager(unittest.TestCase):
    sc: SchemaManager

    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        SchemaManager().clear_all_configurations()
        cls.sc = initSchemaManager()

    def test_register_schema(self):
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

    def test_get_registered_schema(self):
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

    def test_get_python_ref_schema(self):
        schema = SchemaManager().get_schema(schema_identifier="SchemaTestTable1")

        expected_schema = extras.python_test_schema

        self.assertEqual(expected_schema, schema)
