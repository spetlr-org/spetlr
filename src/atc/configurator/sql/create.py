from typing import Dict

from more_itertools import peekable

from atc.configurator.sql import sqlparse
from atc.configurator.sql.db import _walk_db_statement
from atc.configurator.sql.table import _walk_table_statement
from atc.configurator.sql.utils import _meaningful_token_iter


def _walk_create_statement(statement: sqlparse.sql.Statement) -> Dict:
    # create an iterator over all tokens in the statement
    # that are not whitespace
    statement = peekable(_meaningful_token_iter(statement))

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
    if statement.peek().value.upper() == "IF NOT EXISTS":
        next(statement)

    if is_table:
        return _walk_table_statement(statement)
    else:
        return _walk_db_statement(statement)
