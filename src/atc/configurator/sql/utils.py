from . import sqlparse
from .sqlparse.sql import Statement


def _meaningful_token_iter(tks: Statement):
    """Only return tokens that are potentially meaningful sql"""
    for tk in tks:
        if (
            tk.ttype not in sqlparse.tokens.Whitespace
            and tk.ttype not in sqlparse.tokens.Comment
        ):
            yield tk
