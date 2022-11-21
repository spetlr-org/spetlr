from atc.schema_manager import SchemaManager

from .python_schemas import python_test_schema


def initSchemaManager() -> SchemaManager:
    """
    This is the master function to initialize the SchemaManager.
    The code to register the schema with the manager is not carried out as part of the
    importing of this module. Instead, it needs to be done by calling this function.
    There are several design reasons for this.
    - As a function, it is possible to re-initialize the SchemaManager at any time.
    - The timing of the execution of the code can be controlled
    - Less code is executed at the time of importing of the module,
        allowing quicker startup
    - If this module contains no other code, and the initialization
        happens as part of the module import, it may have to be imported for its
        side effects only. This leads to less clear code.
        Therefore, we make this a function.
    """
    sc = SchemaManager()
    sc.register_schema(schema_name="python_test_schema", schema=python_test_schema)
    return sc
