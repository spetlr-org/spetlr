from atc.schema_manager import SchemaManager

from .python_schemas import python_test_schema


def initSchemaManger():
    sc = SchemaManager()
    sc.register_schema("python_test_schema", python_test_schema)
    return sc
