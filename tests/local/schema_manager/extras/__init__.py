from atc.schema_manager import SchemaManager

from .python_schemas import python_test_schema

SchemaManager().register_schema(
    schema_name="python_test_schema", schema=python_test_schema
)
