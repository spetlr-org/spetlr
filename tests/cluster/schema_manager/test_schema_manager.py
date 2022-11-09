import unittest

import pyspark.sql.types as T

from atc.config_master import TableConfigurator
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

        self.assertNotIn("register_test_schema", manager.registered_schemas.keys())

        manager.register_schema(schema_name="register_test_schema", schema=schema)

        self.assertIn("register_test_schema", manager.registered_schemas.keys())

    def test_get_registered_schema(self):
        tc = TableConfigurator()
        tc.add_resource_path(extras)
        manager = SchemaManager()

        schema = extras.python_test_schema

        self.assertEqual(
            manager.get_registered_schema(schema_name="python_test_schema"), schema
        )

    def test_get_python_schema(self):
        str_schema = SchemaManager().get_schema_as_string(table_id="SchemaTestTable1")

        self.assertEqual(str_schema, "a int, b string,")

    def test_get_sql_schema(self):
        str_schema = SchemaManager().get_schema_as_string(table_id="SchemaTestTable2")

        self.assertEqual(str_schema, "a INTEGER, b STRING,")

    # TODO #
    def test_read_json_schema(self):
        pass

    # TODO #
    def test_read_python_schema(self):
        pass

    def test_get_schema_as_string(self):
        pass

    def test_get_all_schemas(self):
        pass

    def test_get_all_spark_sql_schemas(self):
        pass


if __name__ == "__main__":
    unittest.main()
