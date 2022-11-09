import copy
from typing import Dict

import pyspark.sql.types as T

from atc.atc_exceptions import AtcException
from atc.configurator import Configurator
from atc.singleton import Singleton


class NoSuchSchemaException(AtcException):
    pass


class FalseSchemaDefinitionException(AtcException):
    pass


class UnregisteredSchemaDefinitionException(AtcException):
    pass


class SchemaManager(metaclass=Singleton):

    # This dict contains the registered schemas
    _registered_schemas: Dict[str, T.StructType] = {}

    def __init__(self):
        pass

    def register_schema(self, schema_name: str, schema: T.StructType):
        self._registered_schemas[schema_name] = schema

    def get_schema(self, schema_identifier: str):
        # Check if the identifier is a registered schema
        if schema_identifier in self._registered_schemas.keys():
            return self._registered_schemas[schema_identifier]

        # Otherwise, check if the schema identifier is a table identifier
        try:
            schema = Configurator().get(table_id=schema_identifier, property="schema")
        except ValueError:
            raise NoSuchSchemaException(schema_identifier)

        # If the schema is a string, check if it has been registered
        if isinstance(schema, str) and schema in self._registered_schemas.keys():
            return self._registered_schemas[schema]

        # Otherwise, it must be a dict
        if not isinstance(schema, dict):
            raise UnregisteredSchemaDefinitionException(schema)

        dict_items = list(schema.items())
        if len(dict_items) != 1:
            raise FalseSchemaDefinitionException(schema)

        schema_tuple = dict_items[0]
        # Check if the schema is a sql string
        if schema_tuple[0] == "sql":
            return T._parse_datatype_string(schema_tuple[1])

        # TODO: Add additional schema types here
        else:
            raise FalseSchemaDefinitionException(schema)

    def _schema_to_string(self, schema: T.StructType):
        # TODO: Create a more capable method of translating StructTypes to strings
        # Lacking:
        # - generated-always-as
        # - comments

        str_schema = ", ".join(
            [f"{field.name} {field.dataType.simpleString()}" for field in schema.fields]
        )

        return str_schema

    def get_schema_as_string(self, schema_identifier: str):
        schema = self.get_schema(schema_identifier)

        str_schema = self._schema_to_string(schema)

        return str_schema

    def get_all_schemas(self):
        out_schemas = copy.deepcopy(self._registered_schemas)

        table_details = Configurator().get_all_details()

        for name, value in list(table_details.items()):
            if "_schema" in name:
                table_name = name.replace("_schema", "")
                out_schemas[table_name] = value

        return out_schemas

    def get_all_spark_sql_schemas(self):
        schemas_dict = self.get_all_schemas()
        str_schemas = {}

        for name, schema in list(schemas_dict.items()):
            str_schema = self._schema_to_string(schema)
            str_schemas[name] = str_schema

        return str_schemas
