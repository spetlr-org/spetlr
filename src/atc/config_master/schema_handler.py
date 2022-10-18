from types import ModuleType
from typing import List, Union

import pyspark.sql.types as T

from atc.utils.SubModuleMethods import import_objects_of_type


class SchemaHandler:
    def __init__(
        self,
        schema_modules: Union[ModuleType, List[ModuleType]],
        recursive: bool = True,
    ) -> None:
        self.schemas = {}
        self.add_modules(schema_modules, recursive)

    def add_modules(
        self,
        schema_modules: Union[ModuleType, List[ModuleType]],
        recursive: bool = True,
    ):
        if isinstance(schema_modules, List):
            for module in schema_modules:
                results = import_objects_of_type(
                    module, T.StructType, recursive=recursive
                )
                self.schemas.update(results)
        else:
            results = import_objects_of_type(
                schema_modules, T.StructType, recursive=recursive
            )
            self.schemas.update(results)

    def _translate_field(self, field: T.StructField) -> str:
        field_str = f"{field.name} {field.dataType.simpleString()}"

        if "comment" in field.metadata:
            field_str = field_str + f" COMMENT \"{field.metadata['comment']}\""

        return field_str

    def get_schema(self, schema_name: str) -> str:
        schema = self.schemas[schema_name]
        schema_str = ""

        for field in schema.fields[:-1]:
            schema_str = schema_str + self._translate_field(field) + ",\n"

        schema_str = schema_str + self._translate_field(schema.fields[-1])

        return schema_str
