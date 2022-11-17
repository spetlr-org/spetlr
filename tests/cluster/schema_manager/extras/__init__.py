from atc.schema_manager.schema_manager import register_schema

from .python_schemas import python_test_schema

register_schema(schema_name="python_test_schema", schema=python_test_schema)
