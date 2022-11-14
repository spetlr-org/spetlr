import unittest

import pyspark.sql.types as T

from atc.configurator import Configurator
from atc.schema_manager import SchemaManager
from tests.cluster.schema_manager import extras


class TestSchemaManager(unittest.TestCase):
    def test_register_schema(self):
        manager = SchemaManager()

        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )

        self.assertNotIn("register_test", manager._registered_schemas.keys())

        manager.register_schema(schema_name="register_test", schema=schema)

        self.assertIn("register_test", manager._registered_schemas.keys())

    def test_get_registered_schema(self):
        Configurator().add_resource_path(extras)
        manager = SchemaManager()

        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )

        self.assertNotIn("register_test2", manager._registered_schemas.keys())

        manager.register_schema(schema_name="register_test2", schema=schema)

        self.assertEqual(
            manager.get_schema(schema_identifier="register_test2"),
            schema,
        )

    def test_get_python_ref_schema(self):
        Configurator().add_resource_path(extras)

        schema = SchemaManager().get_schema(schema_identifier="SchemaTestTable1")

        expected_schema = extras.python_test_schema

        self.assertEqual(schema, expected_schema)


if __name__ == "__main__":
    unittest.main()
