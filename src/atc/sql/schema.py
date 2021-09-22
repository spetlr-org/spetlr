import string
from typing import List

from more_itertools import peekable
from pyspark.sql import types as t
import re


class SchemaExtractionError(Exception):
    pass


__debug = True

def log(*args, **kwargs):
    if __debug:
        print(*args, **kwargs)

def get_schema(sql: str) -> t.StructType:
    sql = sql.strip()
    # step 1: clean up the code
    if not (sql.startswith("(") and sql.endswith(")")):
        raise SchemaExtractionError(
            "The argument must be a schema block including parentheses ()"
        )
    sql = sql[1:-1].strip()

    # remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.MULTILINE)

    # remove line comments
    sql = re.sub("--.*", "", sql, flags=re.MULTILINE)

    # simplify all whitespace
    sql = re.sub(
        r"\s+", " ", sql, flags=re.MULTILINE
    )  # make all whitespace a single space
    sql = re.sub(
        r"\s*,", ",", sql, flags=re.MULTILINE
    )  # remove whitespace before comma

    # remove comment tags
    sql = re.sub(
        r" COMMENT\s*"  # the COMMENT keyword,
        r'"'  # followed by a "
        r'([^"]|\\")*'  # followed by an number of non-" OR '\"' sequences
        r'",',  # finally followed by the closing ",
        ",",
        sql,
        flags=re.MULTILINE | re.DOTALL | re.IGNORECASE,
    )

    # we can go no further with regex

    it = peekable(sql)
    struct = t.StructType([])
    try:
        while True:
            log("Looking at next field:")
            name = _get_identifier(it)
            log(f"got name {name}")
            _peek_skip_space(it)
            data_type = _get_data_type(it)
            log(f"got name: {name}, type: {data_type}")
            struct.add(name, data_type)
            c = _peek_skip_space(it)
            if next(it) == ",":
                _peek_skip_space(it)
                continue
    except StopIteration:
        pass
    return struct


def _peek_skip_space(it):
    c = it.peek()
    if c == " ":
        next(it)
        c = it.peek()
    return c


def _flatten(it: peekable):
    return "".join(c for c in it)


def _get_data_type(it: peekable) -> t.DataType:
    type_name = _get_data_type_name(it).upper()
    log(f"Type name is {type_name}")
    simples = dict(
        BOOLEAN=t.BooleanType(),
        BYTE=t.ByteType(),
        TINYINT=t.ByteType(),
        SHORT=t.ShortType(),
        SMALLINT=t.ShortType(),
        INT=t.IntegerType(),
        INTEGER=t.IntegerType(),
        LONG=t.LongType(),
        BIGINT=t.LongType(),
        FLOAT=t.FloatType(),
        REAL=t.FloatType(),
        DOUBLE=t.DoubleType(),
        DATE=t.DateType(),
        TIMESTAMP=t.TimestampType(),
        STRING=t.StringType(),
        BINARY=t.BinaryType(),
    )
    if type_name in simples:
        return simples[type_name]
    elif type_name in ["DECIMAL", "DEC", "NUMERIC"]:
        # we need to parse the (scale,precision) block if present
        c = _peek_skip_space(it)
        if c == ",":
            return t.DecimalType()
        if c != "(":
            raise SchemaExtractionError(
                f"invalid character after decimal declaration: {_flatten(it)}"
            )
        next(it)  # consume the (

        # c is (
        block = []
        while it.peek() != ")":
            block.append(next(it))
        next(it)  # consume the )
        _peek_skip_space(it)

        block = "".join(block)
        precision, scale = block.split(",")
        return t.DecimalType(precision=int(precision.strip()), scale=int(scale.strip()))

    elif type_name == "INTERVAL":
        raise NotImplementedError()

    elif type_name == "STRUCT":
        # we need to parse the struct members
        c = next(it)
        if c == " ":
            c = next(it)
        if c != "<":
            raise SchemaExtractionError(
                f"after STRUCT must follow a < > block. found: {c}"
            )

        struct = t.StructType()
        while _peek_skip_space(it) != ">":
            field_name = _get_identifier(it)
            if _peek_skip_space(it) != ":":
                raise SchemaExtractionError(
                    f"Struct Fields must follow the pattern name:type, found after {repr(field_name)}: {_flatten(it)}"
                )
            next(it)  # consume the colon
            field_type = _get_data_type(it)
            # print(f"Struct has field {field_name}:{field_type}")
            struct.add(field_name, field_type)
            if _peek_skip_space(it) == ",":
                next(it)
                continue
            else:
                break
        next(it)
        return struct
    elif type_name == "ARRAY":
        # we need to parse the array element type
        c = next(it)
        if c == " ":
            c = next(it)
        if c != "<":
            raise SchemaExtractionError(
                f"after ARRAY must follow a < > block. found: {_flatten(it)}"
            )
        element_type = _get_data_type(it)
        c = _peek_skip_space(it)
        if c != ">":
            raise SchemaExtractionError(
                f"after ARRAY<{element_type} must follow a >. found: {_flatten(it)}"
            )
        next(it)
        return t.ArrayType(elementType=element_type)
    elif type_name == "MAP":
        # we need to parse the array element type
        c = next(it)
        if c == " ":
            c = next(it)
        if c != "<":
            raise SchemaExtractionError(
                f"after MAP must follow a < > block. found: {_flatten(it)}"
            )
        key_type = _get_data_type(it)
        c = _peek_skip_space(it)
        if c != ",":
            raise SchemaExtractionError(
                f"after MAP<KeyType must follow a ,. found: {_flatten(it)}"
            )
        next(it)
        _peek_skip_space(it)
        value_type = _get_data_type(it)
        c = _peek_skip_space(it)
        if c != ">":
            raise SchemaExtractionError(
                f"after MAP<KeyType,ValueType must follow a >. found: {_flatten(it)}"
            )
        next(it)
        return t.MapType(key_type, value_type)

    else:
        raise NotImplementedError(f"Data type not implementd: {type_name}")


def _get_data_type_name(it: peekable) -> str:
    name = []
    _peek_skip_space(it)

    while it.peek(";") in string.ascii_letters:
        name.append(next(it))

    name = "".join(name).strip()

    if not name:
        raise SchemaExtractionError(f"unable to get a type name: {_flatten(it)}")
    return name


def _get_identifier(it: peekable) -> str:
    escaped = False
    name = []
    first = next(it)

    # let's get this started.
    # I'm using https://spark.apache.org/docs/latest/sql-ref-identifier.html
    if first == "`":
        escaped = True
    elif first in string.ascii_letters:
        name.append(first)
    else:
        SchemaExtractionError("Invalid start of identifier sequence.")

    non_escaped_characters = string.ascii_letters + string.digits + "_"
    non_whitespace = non_escaped_characters + string.punctuation

    while True:
        char = it.peek()

        if char == "`":
            escaped = not escaped
            next(it)
            continue

        if escaped:
            if char in non_whitespace:
                name.append(next(it))
                continue
            else:
                SchemaExtractionError("Whitespace in escaped identifier")

        # by now we are not escaped

        if char in non_escaped_characters:
            name.append(next(it))
            continue
        else:
            return "".join(name)  # don't consume the strange char
