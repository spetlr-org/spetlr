import re
from typing import Dict, List

import yaml

from . import sqlparse
from .sqlparse.sql import Statement

_magic_comment_start = "-- atc.configurator "


def _extract_comment_attributes(stmt: Statement) -> Dict:
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


def _list_no_whitespace(
    tokens: Statement,
) -> List[sqlparse.tokens._TokenType]:
    return [t for t in tokens if t.ttype != sqlparse.tokens.Whitespace]
