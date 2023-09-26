from typing import Dict

from spetlr.configurator.sql.types import _PeekableTokenList


def _walk_view_statement(statement: _PeekableTokenList) -> Dict:
    # for views, we only collect the name.
    name_parts = []
    while True:
        # until we managed to unpack the AS SELECT statement,
        # all the bits must be part of the name.
        # This allows unpacking names like CREATE VIEW {MYDB}.tbl{ID}
        # which will return lots of parsing errors otherwise.

        name_parts.append(next(statement).value.strip("`"))

        peeked = str(statement.peek()).strip().upper()
        if peeked.startswith(("AS", "(", "COMMENT", "TBLPROPERTIES")):
            break
        else:
            continue

    object_details = {"name": "".join(name_parts), "_is_view": True}

    return object_details
