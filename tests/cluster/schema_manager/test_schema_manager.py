import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.configurator import Configurator
from atc.delta import DeltaHandle
from atc.schema_manager import SchemaManager
from atc.schema_manager.spark_schema import get_schema

from . import extras
from .SparkExecutor import SparkSqlExecutor


class TestSchemaManager(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        SchemaManager().clear_all_configurations()
        extras.initSchemaManager()

    def test_get_sql_schema(self):
        schema = SchemaManager().get_schema(schema_identifier="SchemaTestTable2")

        expected_schema = T.StructType(
            [
                T.StructField("a", T.IntegerType(), True),
                T.StructField("b", T.StringType(), True),
            ]
        )

        self.assertEqualSchema(schema, expected_schema)

    def test_get_schema_as_string(self):
        schema = SchemaManager().get_schema_as_string(
            schema_identifier="SchemaTestTable2"
        )

        self.assertEqual(schema, "a int, b string")

    def test_get_all_schemas(self):
        schemas_dict = SchemaManager().get_all_schemas()

        expected_schemas = {
            "python_test_schema": extras.python_test_schema,
            "python_test_schema2": extras.python_test_schema2,
            "SchemaTestTable1": extras.python_test_schema2,
            "SchemaTestTable2": T.StructType(
                [
                    T.StructField("a", T.IntegerType(), True),
                    T.StructField("b", T.StringType(), True),
                ]
            ),
        }

        self.assertDictEqual(expected_schemas, schemas_dict)

    def test_get_all_spark_sql_schemas(self):
        schemas_dict = SchemaManager().get_all_spark_sql_schemas()

        test_schema1_str = (
            "a int, "
            'b int  COMMENT "really? is that it?", '
            "c string, "
            "cplx struct<someId:string,details:struct<id:string>,blabla:array<int>>, "
            "d timestamp, "
            "m map<int,string>, "
            "p decimal(10,3), "
            "final string"
        )
        test_schema2_str = (
            "a int, "
            "c string, "
            "d timestamp, "
            "m map<int,string>, "
            "p decimal(10,3), "
            "final string"
        )
        expected_schemas = {
            "python_test_schema": test_schema1_str,
            "python_test_schema2": test_schema2_str,
            "SchemaTestTable1": test_schema2_str,
            "SchemaTestTable2": "a int, b string",
        }
        self.maxDiff = None
        self.assertDictEqual(expected_schemas, schemas_dict)

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

        self.maxDiff = None
        self.assertEqual(expected_str, transformed_str)

        transformed_schema = get_schema(transformed_str)
        self.maxDiff = None
        self.assertEqualSchema(schema, transformed_schema)

    def test_sql_executor_schema(self):
        SparkSqlExecutor().execute_sql_file("*")

        test_df = DeltaHandle.from_tc("SchemaTestTable1").read()

        self.maxDiff = None
        self.assertEqualSchema(
            SchemaManager().get_schema("python_test_schema2"), test_df.schema
        )
