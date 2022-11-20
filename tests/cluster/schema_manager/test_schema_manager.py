import unittest

import pyspark.sql.types as T

from atc.configurator import Configurator
from atc.delta import DeltaHandle
from atc.schema_manager import SchemaManager

from . import extras
from .SparkExecutor import SparkSqlExecutor


class TestSchemaManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        extras.initSchemaManger()

    def test_get_sql_schema(self):
        schema = SchemaManager().get_schema(schema_identifier="SchemaTestTable2")

        expected_schema = T.StructType(
            [
                T.StructField("a", T.IntegerType(), True),
                T.StructField("b", T.StringType(), True),
            ]
        )

        self.assertEqual(schema, expected_schema)

    def test_get_schema_as_string(self):

        schema = SchemaManager().get_schema_as_string(
            schema_identifier="SchemaTestTable2"
        )

        self.assertEqual(schema, "a int, b string")

    def test_get_all_schemas(self):

        schemas_dict = SchemaManager().get_all_schemas()

        expected_schemas = {
            "python_test_schema": extras.python_test_schema,
            "SchemaTestTable1": extras.python_test_schema,
            "SchemaTestTable2": T.StructType(
                [
                    T.StructField("a", T.IntegerType(), True),
                    T.StructField("b", T.StringType(), True),
                ]
            ),
        }

        self.assertDictEqual(schemas_dict, expected_schemas)

    def test_get_all_spark_sql_schemas(self):
        schemas_dict = SchemaManager().get_all_spark_sql_schemas()

        test_table_string = (
            "a int, "
            "b int, "
            "c string, "
            "cplx struct<someId:string,details:struct<id:string>,blabla:array<int>>, "
            "d timestamp, "
            "m map<int,string>, "
            "p decimal(10,3), "
            "final string"
        )
        expected_schemas = {
            "python_test_table": test_table_string,
            "SchemaTestTable1": test_table_string,
            "SchemaTestTable2": "a int, b string",
        }

        self.assertDictEqual(schemas_dict, expected_schemas)

    def test_schema_to_spark_sql(self):
        schema = T.StructType(
            [
                T.StructField("Column1", T.IntegerType(), True),
                T.StructField("Column2", T.StringType(), True),
                T.StructField("Column3", T.FloatType(), True),
            ]
        )

        transformed_str = SchemaManager()._schema_to_spark_sql(schema=schema)
        expected_str = "Column1 int, Column2 string, Column3 float"
        self.assertEqual(expected_str, transformed_str)

        transformed_schema = T._parse_datatype_string(s=transformed_str)

        self.assertEqual(schema, transformed_schema)

    def test_sql_executor_schema(self):
        SparkSqlExecutor().execute_sql_file("*")

        test_df = DeltaHandle.from_tc("SparkTestTable1").read()

        self.assertEqual(test_df.schema, extras.python_test_schema)
