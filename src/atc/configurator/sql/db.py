import re
from typing import Dict

from atc.configurator.sql.exceptions import _UnpackAttemptFailed
from atc.configurator.sql.StatementBlocks import StatementBlocks
from atc.configurator.sql.substructures import _unpack_options
from atc.configurator.sql.types import _PeekableTokenList
from atc.exceptions.configurator_exceptions import AtcConfiguratorInvalidSqlException


def _walk_db_statement(statement: _PeekableTokenList) -> Dict:
    # for databases, we need to collect all tokens until the next database block matches
    name_parts = []
    blocks = StatementBlocks()
    while True:
        # until we managed to unpack the rest of the database,
        # all the bits must be part of the name.
        # This allows unpacking names like CREATE SCHEMA {baseName}mydb{ID}
        # which will return lots of parsing errors otherwise.

        name_parts.append(next(statement).value.strip("`"))

        try:
            blocks = _extract_db_blocks(statement)
        except _UnpackAttemptFailed:
            continue
        break

    blocks.using = "db"

    # all validation is completed.
    # construct the configurator object

    object_details = blocks.get_simple_structure()
    object_details["name"] = "".join(name_parts)

    return object_details


def _extract_db_blocks(stmt: _PeekableTokenList) -> StatementBlocks:
    """After the initial "create table [if not exists] mytable",
    The statements can contain several blocks that are mostly optional,
    (check the documentation links at the top of this file)
    and which can come in any order.
    (we are not validating SQL here, only extracting information)
    This function extacts those blocks and makes them available.
    """
    blocks = StatementBlocks()
    try:
        while True:
            val = stmt.peek().value.upper()

            if val == "LOCATION":
                next(stmt)
                blocks.location = next(stmt).value.strip("\"'")
                continue

            if val == "COMMENT":
                next(stmt)
                blocks.comment = next(stmt).value.strip("\"'")
                continue

            if re.match(r"WITH\s+DBPROPERTIES", val):
                next(stmt)
                blocks.dbproperties = {}
                for k, v in _unpack_options(stmt):
                    blocks.dbproperties[k] = v
                continue

            if val == ";":
                next(stmt)
                break

            # if we got to here with no exception, break or continue something is wrong
            raise _UnpackAttemptFailed("Unknown statement form encountered.")
    except _UnpackAttemptFailed as e:
        if blocks == StatementBlocks():
            # only raise this exception if no token has been consumed.
            raise
        else:
            raise AtcConfiguratorInvalidSqlException(*(e.args))
    return blocks
