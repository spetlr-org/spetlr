from spetlr.schema_manager import SchemaManager
from tests.cluster.etl.extras.schemas import (
    test_added_col_schema,
    test_base_schema,
    test_col_type_difference_schema,
    test_removed_col_schema,
    test_renamed_col_schema,
)


def initSchemaManager() -> SchemaManager:
    sc = SchemaManager()
    sc.register_schema("test_base_schema", test_base_schema)
    sc.register_schema("test_added_col_schema", test_added_col_schema)
    sc.register_schema(
        "test_col_type_difference_schema", test_col_type_difference_schema
    )
    sc.register_schema("test_removed_col_schema", test_removed_col_schema)
    sc.register_schema("test_renamed_col_schema", test_renamed_col_schema)
    return sc
