"""
This function searches through .sql files and looks for statements to create tables
or databases:
https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-ddl-create-database.html
https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
and returns a dictionary of configuration details.
"""
import importlib.resources
import re
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Dict, Union

import yaml

from ..exceptions.configurator_exceptions import (
    AtcConfiguratorInvalidSqlCommentsException,
    AtcConfiguratorInvalidSqlException,
)
from .sql import parse, sqlparse

_magic_comment_start = "-- atc.configurator "


def _extract_comment_attributes(stmt) -> Dict:
    _magic_comment_start_re = re.compile(_magic_comment_start, re.IGNORECASE)
    collected_lines = []
    for token in stmt:
        if token.ttype in sqlparse.tokens.Whitespace:
            continue
        if token.ttype not in sqlparse.tokens.Comment:
            continue
        val: str = token.value.strip()
        if not _magic_comment_start_re.match(val):
            continue
        collected_lines.append(val[len(_magic_comment_start) :])
    yaml_document = "\n".join(collected_lines)
    return yaml.load(yaml_document, Loader=yaml.FullLoader) or {}


@dataclass
class StatementBlocks:
    schema: str = None
    using: str = None
    options: str = None
    partitioned_by: str = None
    clustered_by: str = None
    sorted_by: str = None
    location: str = None
    comment: str = None
    tblproperties: str = None
    as_: str = None


def _extract_optional_blocks(stmt) -> StatementBlocks:
    """After the initial "create table [if not exists] mytable",
    The statements can contain several blocks that are mostly optional,
    (check the documentation links at the top of this file)
    and which can come in any order.
    (we are not validating SQL here, only extracting information)
    This function extacts those blocks and makes them available.
    """
    blocks = StatementBlocks()
    stmt = iter(stmt)
    for token in stmt:
        val = token.value.upper()
        if val == "USING":
            blocks.using = next(stmt).value
        elif isinstance(token, sqlparse.sql.Parenthesis):
            blocks.schema = token.value
        elif val == "OPTIONS":
            options = next(stmt)
            if not isinstance(options, sqlparse.sql.Parenthesis):
                AtcConfiguratorInvalidSqlException("expected parenthesis with options")
            blocks.options = options.value
        elif val == "PARTITIONED BY":
            partitions = next(stmt)
            if not isinstance(partitions, sqlparse.sql.Parenthesis):
                AtcConfiguratorInvalidSqlException(
                    "expected parenthesis with partitions"
                )
            blocks.options = partitions.value
        elif val == "CLUSTERED BY":
            cluster_cols = next(stmt)
            if not isinstance(cluster_cols, sqlparse.sql.Parenthesis):
                AtcConfiguratorInvalidSqlException(
                    "expected parenthesis with clustering cols"
                )

            cluster_line = cluster_cols.value

            peek = next(stmt)
            if peek.value.upper() == "SORTED BY":
                sort_cols = next(stmt)
                if not isinstance(sort_cols, sqlparse.sql.Parenthesis):
                    AtcConfiguratorInvalidSqlException(
                        "expected parenthesis with sorting cols"
                    )
                cluster_line += "\nSORTED BY " + sort_cols.value
                into_token = next(stmt)
            else:
                into_token = peek

            if not into_token.value.upper() == "INTO":
                AtcConfiguratorInvalidSqlException("expected INTO after clustering")

            num_buckets = int(next(stmt).value)

            buckets_token = next(stmt)
            if not buckets_token.value.upper() == "BUCKETS":
                AtcConfiguratorInvalidSqlException("expected BUCKETS after INTO n")

            cluster_line += f"\nINTO {num_buckets} BUCKETS"
        elif val == "LOCATION":
            blocks.location = next(stmt).value.strip("\"'")
        elif val == "COMMENT":
            blocks.comment = next(stmt).value.strip("\"'")
        elif val == "TBLPROPERTIES ":
            tblproperties = next(stmt)
            if not isinstance(tblproperties, sqlparse.sql.Parenthesis):
                AtcConfiguratorInvalidSqlException(
                    "expected options list after with TBLPROPERTIES"
                )
            blocks.tblproperties = tblproperties.value
        elif val == "AS":
            raise NotImplementedError("Select AS not currently supported")
        elif val == ";":
            break
        else:
            raise AtcConfiguratorInvalidSqlException(
                "Unknown statement form encountered."
            )
    return blocks


def _walk_create_statement(statement) -> Dict:
    # create an iterator over all tokens in the statement
    # that are not whitespace
    statement = (
        token
        for token in statement
        if token.ttype not in sqlparse.tokens.Whitespace
        and token.ttype not in sqlparse.tokens.Comment
    )

    # We are now ready to go through the statement
    # both create Table and create database must start with create
    token = next(statement)
    if token.value.upper() != "CREATE":
        # not a table or db create statement. Nothing to do
        return {}

    entity = next(statement)

    if entity.value.upper() == "TABLE":
        is_table = True
    elif entity.value.upper() in ("SCHEMA", "DATABASE"):
        is_table = False
    else:
        # not a table or db create statement. Nothing to do
        return {}

    # next is the optional "IF NOT EXISTS"
    peek = next(statement)
    if peek.value.upper() == "IF NOT EXISTS":
        name_token = next(statement)
    else:
        name_token = peek

    # the following token must be the name
    if not isinstance(name_token, sqlparse.sql.Identifier):
        raise AtcConfiguratorInvalidSqlException("Expected an identifier")

    name = name_token.value.strip("`")
    # guard against double definition.

    blocks = _extract_optional_blocks(statement)

    if not is_table and blocks.schema is not None:
        raise AtcConfiguratorInvalidSqlException("A database cannot have a schema")

    if is_table and blocks.using is None:
        raise AtcConfiguratorInvalidSqlException("A table must have a using statement")

    # all validation is completed.
    # construct the configurator object

    object_details = {
        "name": name,
        "path": blocks.location,
        "format": blocks.using.lower() if is_table else "db",
        "options": blocks.options,
        "partitioned_by": blocks.partitioned_by,
        "clustered_by": blocks.clustered_by,
        "sorted_by": blocks.clustered_by,
        "comment": blocks.comment,
        "tblproperties": blocks.tblproperties,
        "schema": {"sql": blocks.schema.strip("()")} if blocks.schema else None,
    }
    return {k: v for k, v in object_details.items() if v is not None}


def _parse_sql_to_config(resource_path: Union[str, ModuleType]) -> Dict:
    details = {}
    for file_name in importlib.resources.contents(resource_path):
        extension = Path(file_name).suffix
        if not extension == ".sql":
            continue
        with importlib.resources.path(resource_path, file_name) as file_path:
            with open(file_path) as file:
                for statement in parse(file.read()):

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
