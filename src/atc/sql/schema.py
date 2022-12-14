import dataclasses
import re
import string

from more_itertools import peekable
from pyspark.sql import types as t


class SchemaExtractionError(Exception):
    pass


__debug = False


def log(*args, **kwargs):
    if __debug:
        print(*args, **kwargs)


def get_schema(sql: str) -> t.StructType:
    # remove witespace and parenthesis
    sql = sql.strip().strip("()").strip()

    # remove block comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.MULTILINE)

    # remove line comments
    sql = re.sub("--.*", "", sql, flags=re.MULTILINE)

    # simplify all whitespace
    sql = re.sub(
        r"\s+", " ", sql, flags=re.MULTILINE
    )  # make all whitespace a single space

    for c in [",", ":", "<", ">", r"\(", r"\)"]:
        # remove whitespace before and after spacial character
        sql = re.sub(rf"\s*{c}\s*", c.strip("\\"), sql, flags=re.MULTILINE)

    # we can go no further with regex

    it = peekable(sql)
    struct = t.StructType([])
    try:
        while True:
            name = _get_identifier(it)
            _peek_skip_space_or_colon(it)
            data_type = _get_data_type(it)
            m = _get_metadata(it)
            metadata = {"comment": m.comment} if m.comment else None
            struct.add(
                field=name, data_type=data_type, nullable=m.nullable, metadata=metadata
            )
            if next(it) == ",":
                continue
            else:
                raise SchemaExtractionError("Malformed SQL")
    except StopIteration:
        pass
    return struct


@dataclasses.dataclass
class _Metadata:
    comment: str = None
    nullable: bool = True


def _get_metadata(it) -> _Metadata:
    m = _Metadata()
    while it:
        if it.peek() == ",":
            break  # no more metadata

        if it.peek() != " ":
            raise SchemaExtractionError("Malformed SQL")
        next(it)  # consume space

        token = _get_ascii_token(it).upper()
        if token == "NOT":
            for c in " NULL":
                try:
                    if next(it).upper() != c:
                        raise SchemaExtractionError("Malformed SQL")
                except StopIteration:
                    raise SchemaExtractionError("Malformed SQL")
            m.nullable = False
            continue
        elif token == "COMMENT":
            # manually parse this thing, there must be better ways,
            if not it or next(it) != " ":
                raise SchemaExtractionError("Malformed SQL")
            if not it or next(it) != '"':
                raise SchemaExtractionError("Malformed SQL")

            escape_active = False
            m.comment = ""
            for c in it:
                if escape_active:
                    m.comment += c
                    escape_active = False
                elif c == "\\":
                    escape_active = True
                elif c == '"':
                    break
                else:
                    m.comment += c

    return m


def _peek_skip_space(it):
    c = it.peek()
    if c == " ":
        next(it)
        c = it.peek()
    return c


def _peek_skip_space_or_colon(it):
    c = it.peek("x")
    if c in " :":
        next(it)


def _flatten(it: peekable):
    return "".join(c for c in it)


def _get_data_type(it: peekable) -> t.DataType:
    type_name = _get_ascii_token(it).upper()
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
        c = it.peek("x")
        if c == ",":
            return t.DecimalType()
        if c != "(":
            raise SchemaExtractionError(
                f"invalid character after decimal declaration: {_flatten(it)}"
            )
        next(it)  # consume the (

        # c is (
        block = []
        while True:
            try:
                c = next(it)
            except StopIteration:
                raise SchemaExtractionError("decimal () block never ended")
            if c == ")":
                break
            block.append(c)

        block = "".join(block)
        precision, scale = block.split(",")
        return t.DecimalType(precision=int(precision.strip()), scale=int(scale.strip()))

    elif type_name == "INTERVAL":
        raise NotImplementedError()

    elif type_name == "STRUCT":
        # we need to parse the struct members
        try:
            c = next(it)
        except StopIteration:
            raise SchemaExtractionError(
                "after STRUCT a <> block must follow, but string ended."
            )
        if c != "<":
            raise SchemaExtractionError(
                f"after STRUCT must follow a < > block. found: '{c}'"
            )

        struct = t.StructType()
        # get the struct members. each on is of type {field_name}:{field_type}
        while it.peek() != ">":
            field_name = _get_identifier(it)
            if it.peek("ERROR") != ":":
                raise SchemaExtractionError(
                    "Struct Fields must follow the pattern "
                    f"name:type, found after {repr(field_name)}: {_flatten(it)}"
                )
            next(it)  # consume the colon

            # the type can also be a struct again. Therefore, recurse here.
            field_type = _get_data_type(it)

            struct.add(field_name, field_type)
            c = next(it)
            if c == ",":
                # get next field
                continue
            elif c == ">":
                break
            else:
                raise SchemaExtractionError(f'invalid char "{c}" after schema fields')
        return struct
    elif type_name == "ARRAY":
        # we need to parse the array element type
        c = next(it)

        if c != "<":
            raise SchemaExtractionError(
                f"after ARRAY must follow a < > block. found: {_flatten(it)}"
            )
        element_type = _get_data_type(it)
        c = next(it)
        if c != ">":
            raise SchemaExtractionError(
                f"after ARRAY<{element_type} must follow a >. found: {c}"
            )
        return t.ArrayType(elementType=element_type)
    elif type_name == "MAP":
        # we need to parse the array element type
        c = next(it)

        if c != "<":
            raise SchemaExtractionError(
                f"after MAP must follow a < > block. found: {_flatten(it)}"
            )
        key_type = _get_data_type(it)

        if it.peek() != ",":
            raise SchemaExtractionError(
                f"after MAP<KeyType must follow a ,. found: {_flatten(it)}"
            )
        next(it)

        value_type = _get_data_type(it)

        if it.peek() != ">":
            raise SchemaExtractionError(
                f"after MAP<KeyType,ValueType must follow a >. found: {_flatten(it)}"
            )
        next(it)

        return t.MapType(key_type, value_type)

    else:
        raise NotImplementedError(f"Data type not implementd: {type_name}")


def _get_ascii_token(it: peekable) -> str:
    name = []

    while it.peek(".") in string.ascii_letters:
        name.append(next(it))

    name = "".join(name).strip()

    if not name:
        raise SchemaExtractionError(f"unable to get a token: '{_flatten(it)}'")
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
