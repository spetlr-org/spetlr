import json
from string import Formatter
from typing import Any, Dict

import pyspark.sql.types as T

from spetlr.configurator import Configurator
from spetlr.exceptions import (
    FalseSchemaDefinitionException,
    NoSuchSchemaException,
    NoSuchValueException,
    UnregisteredSchemaDefinitionException,
)
from spetlr.schema_manager.spark_schema import get_schema
from spetlr.singleton import Singleton


class SchemaManager(metaclass=Singleton):
    _DEFAULT = object()
    # This dict contains the registered schemas
    _registered_schemas: Dict[str, T.StructType] = {}

    def __init__(self):
        self.clear_all_configurations()

    def clear_all_configurations(self):
        self._registered_schemas = dict()

    def register_schema(self, schema_name: str, schema: T.StructType) -> None:
        self._registered_schemas[schema_name] = schema

    def get_schema(
        self,
        schema_identifier: str,
        default: Any = _DEFAULT,
    ) -> T.StructType:
        """
        Get a schema from either the registered schemas or the tables available
        to the Configurator.\n
        \"schema_identifier\" accepts either the name of a registered schema or
        a Configurator table id.
        """

        # Check if the identifier was directly registered or previously retrieved
        if schema_identifier in self._registered_schemas.keys():
            return self._registered_schemas[schema_identifier]

        # Otherwise, the schema identifier must be table identifier
        try:
            schema = Configurator().get(table_id=schema_identifier, property="schema")
        except NoSuchValueException:
            if default is self._DEFAULT:
                raise NoSuchSchemaException(schema_identifier)
            else:
                return default

        # If the schema is a string, look it up as another schema
        if isinstance(schema, str):
            # exceptions raised here will correctly roll up to the caller.
            # recursive call allows for stacked definitions
            other_schema = self.get_schema(schema)
            self._registered_schemas[schema_identifier] = other_schema
            return other_schema

        # Otherwise, it must be a dict
        if not isinstance(schema, dict):
            raise UnregisteredSchemaDefinitionException(schema)

        # Ensure that the dict has exactly 1 item
        [(key, value)] = schema.items()

        # Check if the schema is a sql string
        if key == "sql":
            # Check for substitutions like {} contained in the value:
            other_schema_keys = set(
                i[1] for i in Formatter().parse(value) if i[1] is not None
            )
            replacements = {}
            for other_key in other_schema_keys:
                if other_key.endswith("_schema"):
                    other_base_key = other_key[: -len("_schema")]
                    replacements[other_key] = self.get_schema_as_string(other_base_key)
                else:
                    replacements[other_key] = Configurator().get_all_details()[
                        other_key
                    ]
            if replacements:
                value = value.format(**replacements)

            parsed_schema = get_schema(value)
            if not isinstance(parsed_schema, T.StructType):
                raise FalseSchemaDefinitionException()
            self._registered_schemas[schema_identifier] = parsed_schema
            return parsed_schema

        # TODO: Add additional schema types here
        else:
            raise FalseSchemaDefinitionException(schema)

    def struct_to_sql(self, schema: T.StructType, formatted=False) -> str:
        """Convert the given schema into sql rows
        that can form part of a CREATE TABLE statement.
        Includes support for comments, nullability,
        and complex data types (e.g. structs, arrays),

        if formatted is True, the sql will contain newlines
        and be indented for use in a long SQL schema.
        """
        return self._schema_to_spark_sql(schema, formatted=formatted)

    def _schema_to_spark_sql(self, schema: T.StructType, formatted=False) -> str:
        # TODO: Create a more capable method of translating StructTypes to
        # spark sql strings
        # Lacking:
        # - generated-always-as

        rows = []
        for field in schema.fields:
            row = f"{field.name} {field.dataType.simpleString()}"
            if not field.nullable:
                row += " NOT NULL"
            if "comment" in field.metadata:
                # I could have used a repr() here,
                # but then I could get single quoted string. This ensured double quotes
                row += f' COMMENT {json.dumps(field.metadata["comment"])}'
            rows.append(row)

        separator = ",\n  " if formatted else ", "

        str_schema = separator.join(rows)

        return str_schema

    def get_schema_as_string(self, schema_identifier: str) -> str:
        "return schema as a sql schema string"
        schema = self.get_schema(schema_identifier)

        str_schema = self._schema_to_spark_sql(schema)

        return str_schema

    def get_all_schemas(self) -> Dict[str, T.StructType]:
        for id in Configurator().all_keys():
            try:
                self.get_schema(schema_identifier=id)
            except NoSuchSchemaException:
                continue

        return self._registered_schemas

    def get_all_spark_sql_schemas(self) -> Dict[str, str]:
        schemas_dict = self.get_all_schemas()
        str_schemas = {}

        for name, schema in schemas_dict.items():
            str_schema = self._schema_to_spark_sql(schema, formatted=True)
            str_schemas[name] = str_schema

        return str_schemas
