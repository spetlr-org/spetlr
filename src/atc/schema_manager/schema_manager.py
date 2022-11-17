from typing import Dict

import pyspark.sql.types as T

from atc.atc_exceptions import (
    FalseSchemaDefinitionException,
    NoSuchSchemaException,
    UnregisteredSchemaDefinitionException,
)
from atc.configurator import Configurator
from atc.singleton import Singleton


class SchemaManager(metaclass=Singleton):

    # This dict contains the registered schemas
    _registered_schemas: Dict[str, T.StructType] = {}

    def __init__(self):
        pass

    def register_schema(self, schema_name: str, schema: T.StructType) -> None:
        self._registered_schemas[schema_name] = schema

    def get_schema(self, schema_identifier: str) -> T.StructType:
        """
        Get a schema from either the registered schemas or the tables available
        to the Configurator.\n
        \"schema_identifier\" accepts either the name of a registered schema or
        a Configurator table id.
        """

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
            self._registered_schemas[schema_identifier] = self._registered_schemas[
                schema
            ]
            return self._registered_schemas[schema_identifier]

        # Otherwise, it must be a dict
        if not isinstance(schema, dict):
            raise UnregisteredSchemaDefinitionException(schema)

        # Ensure that the dict has exactly 1 item
        [(key, value)] = schema.items()

        # Check if the schema is a sql string
        if key == "sql":
            self._registered_schemas[schema_identifier] = T._parse_datatype_string(
                value
            )
            return self._registered_schemas[schema_identifier]

        # TODO: Add additional schema types here
        else:
            raise FalseSchemaDefinitionException(schema)

    def _schema_to_spark_sql(self, schema: T.StructType) -> str:
        # TODO: Create a more capable method of translating StructTypes to
        # spark sql strings
        # Lacking:
        # - generated-always-as
        # - comments
        # - nested datatypes

        str_schema = ", ".join(
            [f"{field.name} {field.dataType.simpleString()}" for field in schema.fields]
        )

        return str_schema

    def get_schema_as_string(self, schema_identifier: str) -> str:
        schema = self.get_schema(schema_identifier)

        str_schema = self._schema_to_spark_sql(schema)

        return str_schema

    def get_all_schemas(self) -> Dict[str, T.StringType]:
        table_ids = Configurator().table_details.keys()

        for id in table_ids:
            try:
                Configurator().get(id, "schema")
                self.get_schema(schema_identifier=id)
            except Exception:
                continue

        return self._registered_schemas

    def get_all_spark_sql_schemas(self) -> Dict[str, str]:
        schemas_dict = self.get_all_schemas()
        str_schemas = {}

        for name, schema in list(schemas_dict.items()):
            str_schema = self._schema_to_spark_sql(schema)
            str_schemas[name] = str_schema

        return str_schemas


def register_schema(schema_name: str, schema: T.StructField) -> None:
    SchemaManager().register_schema(schema_name=schema_name, schema=schema)
