import json
import re
from typing import Optional

from more_itertools import peekable
from pyspark.sql import types as t

from spetlr.configurator.sql.init_sqlparse import parse
from spetlr.configurator.sql.utils import _meaningful_token_iter


class SchemaExtractionError(Exception):
    pass


def get_schema(sql: str) -> t.StructType:
    """from sql table schema to structType"""
    sql = sql.strip()
    if sql[0] == "(":
        sql = sql[1:-1]
    parsed_schema = parse(sql)
    if not len(parsed_schema) == 1:
        raise SchemaExtractionError("multiple statements")
    (tokenlist,) = parsed_schema

    iter = peekable(_meaningful_token_iter(tokenlist.flatten()))

    return _get_schema(iter)


# throughout this file, the variable iter represents the following object:
# - a peekable iterator
# - over a flattened list of sql tokens
# - where comments and whitespaces have been removed.
# as an iterator it has a state and will therefore refer to some local position
# of a stream at the start of the function. After the function call, it will point
# to the position after the end of the processed tokens (if any)
# any call to next or peek can throw StopIteration, so be sure to always maintain
# a coherent state in case of unexpected function exit.
#
# the underscore methods are private and no TypeHint is used. If you can capture the
# above in a type-hint, please consider a contribution.
def _get_schema(iter) -> t.StructType:
    """Parse everything in the iterator as a struct
    will parse with or without ":" between name and type and will parse
    until the closing > or end of string is detected.
    The closing ">" will be consumed.
    """
    struct = t.StructType()
    # parse the variables one by one
    for token in iter:
        # the first token is taken to be the name.
        # since almost anything can be a name, no validation is done.
        name = token.value.strip("`")

        colon_token = iter.peek()
        if colon_token.value == ":":
            next(iter)  # ignore the colon

        # get the data type including any definitions for struct, arrays, maps
        dataType = _get_data_type(iter)

        # create the member now and modify it further as needed
        # this simplifies early return
        struct.add(name, dataType)

        # try parsing any further modifiers like nullability and comment.
        set_comment = None
        set_nullable = None
        set_default = None
        try:
            # loop over token blocks until either
            # - the next struct member definition starts at ,
            # - or the struct definition ends with ">"
            # - or a StopIteration is raised at the end of the string
            while iter.peek().value not in ",>":
                # GENERATED is simply ignored in this parsing
                if _ignore_generated(iter):
                    continue

                # DEFAULT is simply ignored in this parsing
                default = _get_default(iter)
                if default is not None and not set_default:
                    struct.fields[-1].metadata["CURRENT_DEFAULT"] = default
                    set_default = True
                    continue

                comment = _get_comment(iter)
                if comment is not None and not set_comment:
                    struct.fields[-1].metadata["comment"] = comment
                    set_comment = True
                    continue

                nullable = _get_nullable(iter)
                if nullable is not None and not set_nullable:
                    struct.fields[-1].nullable = nullable
                    set_nullable = True
                    continue

                # if neither of the above made us continue from this loop, get worried
                raise SchemaExtractionError(
                    f"unknown tokens after data type: {iter.peek()}"
                )
        except StopIteration:
            return struct

        # because of the last peek statement, we know that there must be a next value,
        # and it is one of "," or ">"
        final = next(iter).value
        if final == ">":
            # end of struct definition
            break
        elif final == ",":
            # on to the next struct member
            continue
        else:
            raise SchemaExtractionError("Logic error")

    return struct


def schema_to_spark_sql(schema: t.StructType, *, formatted=False) -> str:
    """Convert the given schema into sql rows
    that can form part of a CREATE TABLE statement.
    Includes support for comments, nullability,
    and complex data types (e.g. structs, arrays),

    if formatted is True, the sql will contain newlines
    and be indented for use in a long SQL schema.
    """
    # TODO: Create a more capable method of translating StructTypes to
    # spark sql strings
    # Lacking:
    # - generated-always-as

    rows = []
    for field in schema.fields:
        row = f"{field.name} {field.dataType.simpleString()}"
        if not field.nullable:
            row += " NOT NULL"
        comment = field.metadata.get("comment", None)
        if comment:
            # I could have used a repr() here,
            # but then I could get single quoted string. This ensured double quotes
            row += f" COMMENT {json.dumps(comment)}"
        default_clause = field.metadata.get("CURRENT_DEFAULT", None)
        if default_clause:
            row += f" DEFAULT {default_clause}"
        rows.append(row)

    separator = ",\n  " if formatted else ", "

    str_schema = separator.join(rows)

    return str_schema


def schema_has_any_defaults(schema: t.StructType) -> bool:
    """Return true if any column in the schema has a default value"""
    for field in schema.fields:
        if "CURRENT_DEFAULT" in field.metadata:
            return True
    return False


def _get_nullable(iter) -> Optional[bool]:
    """See if a nullability follows. If it does, return the bool"""
    token = iter.peek()
    if str(token).upper() == "NULL":
        next(token)
        return True
    if re.match(r"NOT\s+NULL", str(token).upper()):
        next(iter)
        return False

    return None


def _get_comment(iter) -> Optional[str]:
    """See if a comment follows. If it does, return the string"""
    token = iter.peek()
    if token.value.upper() != "COMMENT":
        return None
    next(iter)

    comment = next(iter).value
    return comment.strip("'\"")


def _ignore_generated(iter):
    """
    The syntax for GENERATED:
    ( { column_identifier column_type [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expr ) |
        GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [
         ( [ START WITH start ] [ INCREMENT BY step ] )
        ] |
        DEFAULT default_expression ]
      [ COMMENT column_comment ]
      [ column_constraint ] } [, ...]
    [ , table_constraint ] [...] )
    returns true if a GENRATED expression was parsed
    """
    if iter.peek().value.upper() != "GENERATED":
        return False
    next(iter)

    while True:
        peek_val = iter.peek().value.upper()
        if peek_val in ["ALWAYS", "AS", "BY", "DEFAULT", "IDENTITY"]:
            next(iter)
            continue
        if peek_val == "(":
            next(iter)
            _captue_paren(iter, ")")
            continue
        else:
            break

    return True


def _get_default(iter):
    """
    The syntax for GENERATED:
    ( { column_identifier column_type [ NOT NULL ]
      [ GENERATED ALWAYS AS ( expr ) |
        GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [
            ( [ START WITH start ] [ INCREMENT BY step ] )
        ] |
        DEFAULT default_expression ]
      [ COMMENT column_comment ]
      [ column_constraint ] } [, ...]
    [ , table_constraint ] [...] )
    returns the value if a DEFAULT expression was parsed
    """

    if iter.peek().value.upper() != "DEFAULT":
        return None
    next(iter)

    # capture to next comma
    parts = []
    while True:
        try:
            peek_val = iter.peek().value.upper()
        except StopIteration:
            break
        if peek_val == ",":
            break
        parts.append(next(iter))
        if peek_val == "(":
            parts.append(_captue_paren(iter, ")"))
            continue

    default = " ".join(str(p) for p in parts)
    return default


def _captue_paren(iter, closing=")") -> str:
    parts = []
    for token in iter:
        parts.append(token.value)

        if token.value == "(":
            parts.append(_captue_paren(iter, ")"))
        elif token.value == closing:
            break
    return " ".join(parts)


def _get_data_type(iter) -> t.DataType:
    """Interpret the next word as a data type,
    then convert it to pyspark, consuming further tokens if necessary."""
    type_name_token = next(iter)
    type_name = type_name_token.value.upper()

    # Here below is a giant switch statement.
    # For each block, if it matches, it either returns or raises
    try:
        return dict(
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
        )[type_name]
    except KeyError:
        pass

    if type_name in ["DECIMAL", "DEC", "NUMERIC"]:
        # we need to parse the (scale,precision) block if present
        paren_token = iter.peek()

        if paren_token.value != "(":
            return t.DecimalType()
        next(iter)

        precision = next(iter).value
        if next(iter).value != ",":
            raise SchemaExtractionError("expected comma after decimal scale")
        scale = next(iter).value
        if next(iter).value != ")":
            raise SchemaExtractionError("decimal precision section not properly closed")
        return t.DecimalType(precision=int(precision.strip()), scale=int(scale.strip()))

    if type_name == "INTERVAL":
        raise NotImplementedError()

    if type_name == "STRUCT":
        # we need to parse the struct members
        token = next(iter)
        if token.value != "<":
            raise SchemaExtractionError("Struct definition missing")
        return _get_schema(iter)

    if type_name == "ARRAY":
        token = next(iter)
        if token.value != "<":
            raise SchemaExtractionError("Array definition missing")
        element_type = _get_data_type(iter)
        token = next(iter)
        if token.value != ">":
            raise SchemaExtractionError("Array definition not closed as expected.")
        return t.ArrayType(elementType=element_type)

    if type_name == "MAP":
        token = next(iter)
        if token.value != "<":
            raise SchemaExtractionError("map definition missing")
        key_type = _get_data_type(iter)
        token = next(iter)
        if token.value != ",":
            raise SchemaExtractionError("map definition missing comma after key type")

        value_type = _get_data_type(iter)
        token = next(iter)
        if token.value != ">":
            raise SchemaExtractionError("map definition not closed as expected.")

        return t.MapType(key_type, value_type)

    raise NotImplementedError(f"Data type not implementd: {type_name}")


def remove_nullability(schema: t.StructType) -> t.StructType:
    """Return a schema where the nullability of all fields is reset to default"""
    fields = []
    for f in schema.fields:
        fields.append(
            t.StructField(
                name=f.name, dataType=f.dataType, nullable=True, metadata=f.metadata
            )
        )
    return t.StructType(fields)


def remove_empty_comments(schema: t.StructType) -> t.StructType:
    """Return a schema where the comment metadata is dropped if it is empty"""
    fields = []
    for f in schema.fields:
        fields.append(
            t.StructField(
                name=f.name,
                dataType=f.dataType,
                nullable=f.nullable,
                metadata={k: v for k, v in f.metadata.items() if k != "comment" or v},
            )
        )
    return t.StructType(fields)


def simplify_column_defaults(schema: t.StructType) -> t.StructType:
    """
    For columns that have a default value,
    the metadata seems to contain three values
        CURRENT_DEFAULT:
          seems to contain the exact value and spelling as used in
          the DEFAULT claus of the table creation
          example: 'CURRENT_tIMEsTAMP()'
        EXISTS_DEFAULT:
          seems to contain the value of the expression if it were
          used right now. example "TIMESTAMP '2023-10-10 14:55:11.315'"
        default:
          maybe a standardize form of the manually given default.
          example 'current_timestamp()'

    I have no simple way of extracting the values of
    EXISTS_DEFAULT or 'default' for comparison. Therefore,
    I will standardize the schema to only keep the value of
    CURRENT_DEFAULT and compare those exactly.
    """
    removed_keys = ["EXISTS_DEFAULT", "default"]
    fields = []
    for f in schema.fields:
        fields.append(
            t.StructField(
                name=f.name,
                dataType=f.dataType,
                nullable=f.nullable,
                metadata={k: v for k, v in f.metadata.items() if k not in removed_keys},
            )
        )
    return t.StructType(fields)
