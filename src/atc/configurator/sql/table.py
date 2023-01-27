import re
from typing import Dict

from atc.exceptions.configurator_exceptions import AtcConfiguratorInvalidSqlException

from . import sqlparse
from .exceptions import _UnpackAttemptFailed
from .StatementBlocks import ClusteredBy, StatementBlocks
from .substructures import (
    _unpack_list_of_single_variables,
    _unpack_list_of_sorting_statements,
    _unpack_options,
)
from .types import _PeekableTokenList


def _walk_table_statement(statement: _PeekableTokenList) -> Dict:
    # for tables, we need to collect all tokens until the schema starts.
    name_parts = []
    blocks = StatementBlocks()
    while True:
        # until we managed to unpack the rest of the table,
        # all the bits must be part of the name.
        # This allows unpacking names like CREATE TABLE {MYDB}.tbl{ID}
        # which will return lots of parsing errors otherwise.

        name_parts.append(next(statement).value.strip("`"))

        try:
            blocks = _extract_table_blocks(statement)
        except _UnpackAttemptFailed:
            continue
        break

    object_details = blocks.get_simple_structure()
    object_details["name"] = "".join(name_parts)

    return object_details


def _extract_table_blocks(stmt: _PeekableTokenList) -> StatementBlocks:
    """After the initial "create table [if not exists] mytable",
    The statements can contain several blocks that are mostly optional,
    (check the documentation links at the top of this file)
    and which can come in any order.
    (we are not validating SQL here, only extracting information)
    This function extracts those blocks and makes them available.

    (If the unpacking fails before the first token is consumed,
    raise the _UnpackAttemptFailed exception, to allow further
    processing.)
    """

    blocks = StatementBlocks()

    if isinstance(stmt.peek(), sqlparse.sql.Parenthesis):
        blocks.schema = next(stmt).value
    else:
        raise _UnpackAttemptFailed("No valid table schema")

    token = next(stmt)
    val = token.value.upper()
    if val == "USING":
        blocks.using = next(stmt).value
    else:
        raise AtcConfiguratorInvalidSqlException("No valid table data source")

    for token in stmt:
        val = token.value.upper()

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

        if val == "AS":
            raise NotImplementedError("Select AS not currently supported")

        if val == ";":
            break

        # if we got to here with no exception, break or continue something is wrong
        raise AtcConfiguratorInvalidSqlException("Unknown statement form encountered.")
    return blocks
