from typing import List

from .sqlparse import engine, keywords, tokens
from .sqlparse.engine import grouping
from .sqlparse.lexer import Lexer
from .sqlparse.sql import Statement

lex = Lexer.get_default_instance()
my_regex = [
    (r"IF\s+NOT\s+EXISTS\b", tokens.Keyword),
    (r"ZORDER\s+BY\b", tokens.Keyword),
    (r"PARTITIONED\s+BY\b", tokens.Keyword),
    (r"SORTED\s+BY\b", tokens.Keyword),
    (r"CLUSTERED\s+BY\b", tokens.Keyword),
    (r"WITH\s+DBPROPERTIES\b", tokens.Keyword),
    (r"BLOOMFILTER\s+INDEX\b", tokens.Keyword),
    (r"(DEEP|SHALLOW)\s+CLONE\b", tokens.Keyword),
    (r"(MSCK|FSCK)\s+REPAIR\b", tokens.Keyword),
    (r"[<>=~!]", tokens.Operator.Comparison),  # avoid >> being parsed as one token
]

KEYWORDS_DBX = {
    "BLOOMFILTER": tokens.Keyword,
    "BUCKETS": tokens.Keyword,
    "DBPROPERTIES": tokens.Keyword,
    "DETAIL": tokens.Keyword,
    "HISTORY": tokens.Keyword,
    "METADATA": tokens.Keyword,
    "MSCK": tokens.Keyword,
    "OPTIMIZE": tokens.Keyword,
    "PARTITIONS": tokens.Keyword,
    "REFRESH": tokens.Keyword,
    "REPAIR": tokens.Keyword,
    "SYNC": tokens.Keyword,
    "VACUUM": tokens.Keyword,
    "ZORDER": tokens.Keyword,
}


lex.clear()

lex.set_SQL_REGEX(keywords.SQL_REGEX[:38] + my_regex + keywords.SQL_REGEX[38:])
lex.add_keywords(keywords.KEYWORDS_COMMON)
lex.add_keywords(keywords.KEYWORDS_ORACLE)
lex.add_keywords(keywords.KEYWORDS_PLPGSQL)
lex.add_keywords(keywords.KEYWORDS_HQL)
lex.add_keywords(keywords.KEYWORDS_MSACCESS)
lex.add_keywords(keywords.KEYWORDS)

lex.add_keywords(KEYWORDS_DBX)


def mygrouping(stmt):
    for func in [
        # group_comments,
        # _group_matching
        grouping.group_brackets,
        grouping.group_parenthesis,
        grouping.group_case,
        grouping.group_if,
        grouping.group_for,
        grouping.group_begin,
        grouping.group_functions,
        grouping.group_where,
        grouping.group_period,
        grouping.group_arrays,
        grouping.group_identifier,
        grouping.group_order,
        grouping.group_typecasts,
        grouping.group_tzcasts,
        grouping.group_typed_literal,
        grouping.group_operator,
        grouping.group_comparison,
        grouping.group_as,
        grouping.group_aliased,
        grouping.group_assignment,
        grouping.align_comments,
        grouping.group_identifier_list,
        grouping.group_values,
    ]:
        func(stmt)
    return stmt


def parsestream(stream, encoding=None):
    stack = engine.FilterStack()
    # stack.enable_grouping()
    for stmt in stack.run(stream, encoding):
        mygrouping(stmt)
        yield stmt


def parse(sql: str, encoding=None) -> List[Statement]:
    return list(parsestream(sql, encoding))
