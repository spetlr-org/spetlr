"""
This function searches through .sql files and looks for statements to create tables
or databases:
https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-ddl-create-database.html
https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
and returns a dictionary of configuration details.
"""
import importlib.resources
from pathlib import Path
from types import ModuleType
from typing import Dict, Union

from atc.exceptions.configurator_exceptions import (
    AtcConfiguratorInvalidSqlCommentsException,
)

from .comments import _extract_comment_attributes
from .create import _walk_create_statement
from .init_sqlparse import parse


def _parse_sql_to_config(resource_path: Union[str, ModuleType]) -> Dict:
    details = {}
    for file_name in importlib.resources.contents(resource_path):
        extension = Path(file_name).suffix
        if not extension == ".sql":
            continue
        with importlib.resources.path(resource_path, file_name) as file_path:
            with open(file_path) as file:
                sql_code = file.read()

                # the sequence "-- COMMAND ----------" is used in jupyter notebooks
                # and separates cells.
                # We treat it as another way to end a statement
                sql_code = sql_code.replace("-- COMMAND ----------", ";")

                for statement in parse(sql_code):
                    comment_attributes = _extract_comment_attributes(statement)
                    if "key" not in comment_attributes:
                        # if no magic comments were used on the statement,
                        # then there is nothing to configure here
                        continue
                    table_id = comment_attributes.pop("key")

                    object_details = _walk_create_statement(statement)

                    for key in object_details:
                        if key in comment_attributes:
                            raise AtcConfiguratorInvalidSqlCommentsException(
                                f"Error for {key} in {table_id}, "
                                "The comments must not specify attributes, "
                                "that can be derived directly from the sql."
                            )
                    object_details.update(comment_attributes)
                    details[table_id] = object_details
    return details
