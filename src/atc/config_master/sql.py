import re
import sqlparse
from .details import Details
from .exceptions import SqlParseException


def cleanup_sql(sql: str) -> str:
    lines = []
    for line in sql.splitlines(keepends=False):
        line = line.strip()
        if line.startswith("--"):
            continue
        loc = line.find("--")
        if loc >= 0:
            line = line[:loc]
        lines.append(line)
    sql = (" ".join(lines)).strip()
    return re.sub("[\n\r\t ]+", " ", sql)


def sql_is_table(sql: str) -> bool:
    ct = "CREATE TABLE"
    cet = "CREATE EXTERNAL TABLE"
    if ct == sql[: len(ct)].upper() or cet == sql[: len(cet)].upper():
        return True
    return False


def sql_is_db(sql: str) -> bool:
    cd = "CREATE DATABASE"
    cs = "CREATE SCHEMA"
    if cd == sql[: len(cd)].upper() or cs == sql[: len(cs)].upper():
        return True
    return False


def get_details_from_sql(sql: str) -> Details:
    sql = cleanup_sql(sql)
    if sql_is_table(sql):
        is_table = True
    elif sql_is_db(sql):
        is_table = False
    else:
        raise SqlParseException()

    tree = sqlparse.parse(sql)[0]
    clean_tree = [
        token for token in tree if not token.is_whitespace
    ]  # we don't want whitespaces

    # the first identifier is the name
    name_token = [
        token for token in clean_tree if isinstance(token, sqlparse.sql.Identifier)
    ][0]
    name = name_token.value.strip('"')

    if is_table:
        db_name = name.split(".")[0]
    else:
        db_name = None

    # by inspection of the CREATE TABLE syntax found here:
    # https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table.html
    # the schema will always be a parenthesis that starts right after the name
    sql_schema_token_candidate = clean_tree[clean_tree.index(name_token) + 1]
    if is_table and isinstance(sql_schema_token_candidate, sqlparse.sql.Parenthesis):
        table_sql_schema = str(sql_schema_token_candidate).strip().strip("()").strip()
    else:
        table_sql_schema = None

    # get the path
    path_token = [token for token in clean_tree if token.value.upper() == "LOCATION"][0]
    path = clean_tree[clean_tree.index(path_token) + 1].value.strip('"')

    return Details(
        name=name,
        path=path,
        sql_schema=table_sql_schema,
        create_statement=sql,
        is_table=is_table,
        db_name=db_name,
    )
