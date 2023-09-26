from typing import Dict

import sqlparse
from more_itertools import peekable

from spetlr.configurator.sql.db import _walk_db_statement
from spetlr.configurator.sql.table import _walk_table_statement
from spetlr.configurator.sql.utils import _meaningful_token_iter
from spetlr.configurator.sql.view import _walk_view_statement


def _walk_create_statement(statement: sqlparse.sql.Statement) -> Dict:
    # create an iterator over all tokens in the statement
    # that are not whitespace
    statement = peekable(_meaningful_token_iter(statement))

    # We are now ready to go through the statement
    # both create Table and create database must start with create
    token = next(statement)
    if token.value.upper() not in ("CREATE", "CREATE OR REPLACE"):
        # not a create statement. Nothing to do
        return {}

    is_table = False
    is_db = False
    is_view = False
    while True:
        entity = next(statement)
        if entity.value.upper() == "TABLE":
            is_table = True
            break
        elif entity.value.upper() in ("SCHEMA", "DATABASE"):
            is_db = True
            break
        elif entity.value.upper() in ("VIEW",):
            is_view = True
            break
        elif entity.value.upper() in ("TEMPORARY",):
            # simply ignored
            continue
        else:
            # not a table or db create statement. Nothing to do
            return {}

    # next is the optional "IF NOT EXISTS"
    if statement.peek().value.upper() == "IF NOT EXISTS":
        next(statement)

    if is_table:
        return _walk_table_statement(statement)
    if is_db:
        return _walk_db_statement(statement)
    if is_view:
        return _walk_view_statement(statement)

    raise Exception("unreachable code point")
