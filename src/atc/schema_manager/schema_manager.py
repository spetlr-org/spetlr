from typing import Dict

import pyspark.sql.types as T

from atc.atc_exceptions import AtcException
from atc.config_master import TableConfigurator
from atc.singleton import Singleton


class NoSuchSchemaException(AtcException):
    pass


class FalseSchemaDefinitionException(AtcException):
    pass


class SchemaManager(metaclass=Singleton):

    # This dict contains the registered schemas
    registered_schemas: Dict[str, T.StructType]

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        self.registered_schemas = {}

    def register_schema(self, schema_name: str, schema: T.StructType) -> T.StructType:
        self.registered_schemas[schema_name] = schema

    def get_registered_schema(self, schema_name: str) -> T.StructType:
        if schema_name not in self.registered_schemas:
            raise NoSuchSchemaException(property)

        return self.registered_schemas[schema_name]

    def get_schema_as_string(self, table_id: str) -> str:
        schema = TableConfigurator().table_property(
            table_id=table_id, property_name="schema"
        )

        # Check if the schema is a python reference
        if isinstance(schema, str):
            if schema in self.registered_schemas:
                schema = self.registered_schemas[schema]

        elif isinstance(schema, dict):
            dict_items = list(schema.items())

            if len(dict_items) != 1:
                raise FalseSchemaDefinitionException(property)

            dict_tuple = dict_items[0]
            print(dict_tuple)
            # Check if the schema is a sql string
            if dict_tuple[0] == "sql":
                print(dict_tuple[1])
                schema = T._parse_datatype_string(dict_tuple[1])
                print(schema)
            # TODO: Add additionaly schema types here

        return schema
