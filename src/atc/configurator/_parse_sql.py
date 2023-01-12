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
from typing import Dict, Generator, Iterable, Iterator, List, Tuple, Union

import yaml

from ..exceptions.configurator_exceptions import (
    AtcConfiguratorInvalidSqlCommentsException,
    AtcConfiguratorInvalidSqlException,
)
from .sql import parse, sqlparse

_magic_comment_start = "-- atc.configurator "


def _meaningful_token_iter(tks):
    """Only return tokens that are potentially meaningful sql"""
    for tk in tks:
        if (
            tk.ttype not in sqlparse.tokens.Whitespace
            and tk.ttype not in sqlparse.tokens.Comment
        ):
            yield tk


def _list_no_whitespace(
    tokens: Iterable[sqlparse.tokens._TokenType],
) -> List[sqlparse.tokens._TokenType]:
    return [t for t in tokens if t.ttype != sqlparse.tokens.Whitespace]


def _extract_comment_attributes(stmt: sqlparse.sql.Statement) -> Dict:
    _magic_comment_start_re = re.compile(_magic_comment_start, re.IGNORECASE)
    collected_lines = []
    for token in _list_no_whitespace(stmt):
        if token.ttype not in sqlparse.tokens.Comment:
            continue
        val: str = token.value.strip()
        if not _magic_comment_start_re.match(val):
            continue
        collected_lines.append(val[len(_magic_comment_start) :])
    yaml_document = "\n".join(collected_lines)
    return yaml.load(yaml_document, Loader=yaml.FullLoader) or {}


ENUM_ASC = 1
ENUM_DESC = 2


@dataclass
class SortingCol:
    name: str
    ordering: int = ENUM_ASC


@dataclass
class ClusteredBy:
    clustering_cols: List[str]
    n_buckets: int = 0
    sorting: List[SortingCol] = None


@dataclass
class StatementBlocks:
    schema: str = None
    using: str = None
    options: Dict[str, str] = None
    partitioned_by: List[str] = None
    clustered_by: ClusteredBy = None
    location: str = None
    comment: str = None
    tblproperties: Dict[str, str] = None
    dbproperties: Dict[str, str] = None

    def get_simple_structure(self):
        object_details = {
            "path": self.location,
            "format": self.using.lower() if self.using else None,
            "options": self.options,
            "partitioned_by": self.partitioned_by,
            "comment": self.comment,
            "tblproperties": self.tblproperties,
        }

        if self.schema:
            object_details["schema"] = {"sql": self.schema.strip("()")}
            object_details["_raw_sql_schema"] = self.schema.strip("()")

        if self.clustered_by:
            object_details["clustered_by"] = {
                "cols": self.clustered_by.clustering_cols,
                "buckets": self.clustered_by.n_buckets,
            }
            if self.clustered_by.sorting:
                object_details["clustered_by"]["sorted_by"] = []
                for col in self.clustered_by.sorting:
                    object_details["clustered_by"]["sorted_by"].append(
                        dict(
                            name=col.name,
                            ordering=("ASC" if col.ordering == ENUM_ASC else "DESC"),
                        )
                    )

        return {k: v for k, v in object_details.items() if v is not None}


def _unpack_comma_separated_list_in_parens(
    stmt: Iterator[sqlparse.tokens._TokenType],
) -> Generator[List[sqlparse.tokens._TokenType], None, None]:
    grouped_token = next(stmt)
    if not isinstance(grouped_token, sqlparse.sql.Parenthesis):
        AtcConfiguratorInvalidSqlException("expected parenthesis with list")

    it = _meaningful_token_iter(grouped_token.flatten())
    for token in it:
        if token.value in "()":
            continue
        between_commas = [token]
        for token in it:
            if token.value == "," or token.value in "()":
                break
            between_commas.append(token)
        yield between_commas


def _unpack_list_of_single_variables(stmt: Iterator[sqlparse.tokens._TokenType]):
    for tokens in _unpack_comma_separated_list_in_parens(stmt):
        if len(tokens) != 1:
            AtcConfiguratorInvalidSqlException(
                "unexpected number of statements between commas"
            )
        yield tokens[0]


def _unpack_list_of_sorting_statements(stmt: Iterator[sqlparse.tokens._TokenType]):
    for tokens in _unpack_comma_separated_list_in_parens(stmt):
        if len(tokens) not in [1, 2]:
            AtcConfiguratorInvalidSqlException(
                "unexpected number of statements between commas"
            )

        sort_col = SortingCol(tokens[0].value)
        if len(tokens) == 2:
            direction = tokens[1]
            if direction.value.upper() == "ASC":
                pass
            elif direction.value.upper() == "DESC":
                sort_col.ordering = ENUM_DESC
            else:
                AtcConfiguratorInvalidSqlException("unexpected ordering direction")
        yield sort_col


def _unpack_options(
    stmt: Iterator[sqlparse.tokens._TokenType],
) -> Generator[Tuple[str, str], None, None]:
    for tokens in _unpack_comma_separated_list_in_parens(stmt):
        if len(tokens) != 3 or tokens[1].value != "=":
            raise AtcConfiguratorInvalidSqlException("expected assignments")
        key, _, value = tokens
        yield key.value.strip("\"'"), value.value.strip("\"'")


def _extract_optional_blocks(stmt: sqlparse.sql.Statement) -> StatementBlocks:
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
            continue

        if isinstance(token, sqlparse.sql.Parenthesis):
            blocks.schema = token.value
            continue

        if val == "OPTIONS":
            blocks.options = {}
            for k, v in _unpack_options(stmt):
                blocks.options[k] = v
            continue

        if re.match(r"PARTITIONED\s+BY", val):
            blocks.partitioned_by = [
                token.value for token in _unpack_list_of_single_variables(stmt)
            ]
            continue

        if re.match(r"CLUSTERED\s+BY", val):
            blocks.clustered_by = ClusteredBy(
                [token.value for token in _unpack_list_of_single_variables(stmt)]
            )

            peek = next(stmt)
            if re.match(r"SORTED\s+BY", peek.value.upper()):
                blocks.clustered_by.sorting = []
                for sort_col in _unpack_list_of_sorting_statements(stmt):
                    blocks.clustered_by.sorting.append(sort_col)
                into_token = next(stmt)
            else:
                into_token = peek

            if not into_token.value.upper() == "INTO":
                AtcConfiguratorInvalidSqlException("expected INTO after clustering")

            try:
                blocks.clustered_by.n_buckets = int(next(stmt).value)
            except ValueError:
                raise AtcConfiguratorInvalidSqlException(
                    "Unable to extract ordering buckets"
                )

            buckets_token = next(stmt)
            if not buckets_token.value.upper() == "BUCKETS":
                AtcConfiguratorInvalidSqlException("expected BUCKETS after INTO n")
            continue

        if val == "LOCATION":
            blocks.location = next(stmt).value.strip("\"'")
            continue

        if val == "COMMENT":
            blocks.comment = next(stmt).value.strip("\"'")
            continue

        if val == "TBLPROPERTIES":
            blocks.tblproperties = {}
            for k, v in _unpack_options(stmt):
                blocks.tblproperties[k] = v
            continue

        if val == "DBPROPERTIES":
            blocks.dbproperties = {}
            for k, v in _unpack_options(stmt):
                blocks.dbproperties[k] = v
            continue

        if val == "AS":
            raise NotImplementedError("Select AS not currently supported")

        if val == ";":
            break

        print(val)
        # if we got to here with no exception, break or continue something is wrong
        raise AtcConfiguratorInvalidSqlException("Unknown statement form encountered.")
    return blocks


def _walk_create_statement(statement: sqlparse.sql.Statement) -> Dict:
    # create an iterator over all tokens in the statement
    # that are not whitespace
    statement: Iterator[sqlparse.tokens._TokenType] = _meaningful_token_iter(statement)

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

    if is_table:
        if blocks.using is None:
            raise AtcConfiguratorInvalidSqlException(
                "A table must have a using statement"
            )
    else:
        blocks.using = "db"

    # all validation is completed.
    # construct the configurator object

    object_details = blocks.get_simple_structure()
    object_details["name"] = name

    return object_details


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
