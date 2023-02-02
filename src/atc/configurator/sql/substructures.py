from typing import Generator, List, Tuple

from atc.exceptions.configurator_exceptions import AtcConfiguratorInvalidSqlException

from . import sqlparse
from .StatementBlocks import ENUM_DESC, SortingCol
from .types import _PeekableTokenList
from .utils import _meaningful_token_iter


def _unpack_comma_separated_list_in_parens(
    stmt: _PeekableTokenList,
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


def _unpack_list_of_single_variables(stmt: _PeekableTokenList):
    for tokens in _unpack_comma_separated_list_in_parens(stmt):
        if len(tokens) != 1:
            AtcConfiguratorInvalidSqlException(
                "unexpected number of statements between commas"
            )
        yield tokens[0]


def _unpack_list_of_sorting_statements(stmt: _PeekableTokenList):
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
    stmt: _PeekableTokenList,
) -> Generator[Tuple[str, str], None, None]:
    for tokens in _unpack_comma_separated_list_in_parens(stmt):
        statement = str(sqlparse.sql.TokenList(tokens))
        assignment = statement.split("=")

        if len(assignment) != 2:
            raise AtcConfiguratorInvalidSqlException(
                f"expected assignments, got {statement}"
            )

        key, value = assignment

        yield key.strip("\"'"), value.strip("\"'")
