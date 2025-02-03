import itertools
import re
from importlib import resources as ir
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Optional, Union

import sqlparse

from spetlr.configurator.configurator import Configurator
from spetlr.configurator.sql.init_sqlparse import parse
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark
from spetlr.sql import BaseExecutor


class SqlExecutor:
    _DEFAULT = object()

    def __init__(
        self,
        base_module: Union[str, ModuleType] = None,
        server: BaseExecutor = None,
        statement_spliter: Optional[List[str]] = _DEFAULT,
        *,
        ignore_empty_folder: bool = False,
        ignore_lines_regex: Optional[List[str]] = None,
        ignore_location: bool = False,
    ):
        """Class to pre-treat sql statements and execute them.
        Replacement sequences related to the Configurator will be inserted before
        execution.
        Providing no `statement_spliter`
        is equivalent to [";", "-- COMMAND ----------"].
        To disable splitting statements, supply `statement_spliter = None`.
        Semicolon will be treated correctly when quoted or in comments.

        Default behavior supports Spark, which will complain if given
        no actual SQL code or multiple statements.
        """

        self.base_module = base_module
        self.server = server
        if statement_spliter is self._DEFAULT:
            # The sequence "-- COMMAND ----------" is used in Jupyter notebooks
            # and separates cells. We treat it as another way to end a statement.
            statement_spliter = [";", "-- COMMAND ----------"]
        self.statement_spliter = statement_spliter
        self.ignore_empty_folder = ignore_empty_folder

        # Initialize regex-based ignore list
        self.ignore_lines_regex = ignore_lines_regex or []

        # If ignore_location is enabled, add regex pattern for LOCATION
        if ignore_location:
            self.ignore_lines_regex.append(r"^LOCATION\b.*")

    def _wildcard_string_to_regexp(self, instr: str) -> str:
        """Converts a wildcard string pattern into a regular expression."""
        if instr.endswith(".sql"):
            instr = instr[:-4]

        instr = instr.replace("*", ".*")

        if not instr.endswith("$"):
            instr = instr + "$"

        return instr

    def _handle_line_regex(self, cleaned_statement: str) -> str:
        """
        Remove full lines if they match any regex pattern in `ignore_lines_regex`.
        It is expected, that the text parsed have no line splitters in it
        (Unless it is e.g. as comments)
        """

        if not self.ignore_lines_regex:
            return cleaned_statement
        cleaned_lines = []
        for line in cleaned_statement.splitlines(keepends=True):  # Preserve newlines
            stripped_for_check = line.lstrip()  # Preserve indentation

            # Check if the line matches any ignore pattern
            if any(
                re.match(pattern, stripped_for_check)
                for pattern in self.ignore_lines_regex
            ):
                pass  # Ignore the line
            else:
                cleaned_lines.append(line)

        return "".join(cleaned_lines)  # Return cleaned SQL

    def get_statements(
        self,
        file_pattern: str,
        exclude_pattern: str = None,
        replacements: Dict[str, str] = None,
    ):
        """Retrieve SQL statements from files based on patterns."""
        for conts in self._get_raw_contents(
            file_pattern=file_pattern, exclude_pattern=exclude_pattern
        ):
            for statement in self.chop_and_substitute(conts, replacements=replacements):
                yield statement

    def chop_and_substitute(
        self,
        raw_sql: str,
        replacements: Dict[str, str] = None,
    ):
        """given the raw contents of a sql file, break it down into statements
        and execute all substitutions."""
        if replacements is None:
            replacements = {}

        # prepare the full set of replacements
        schema_replacements = {
            f"{k}_schema": v
            for k, v in SchemaManager().get_all_spark_sql_schemas().items()
        }
        replacements = {
            **(Configurator().get_all_details()),
            **schema_replacements,
            **replacements,
        }

        sql_code = raw_sql.format(**replacements)

        if self.statement_spliter is None:
            code_parts = [sql_code]
        elif ";" not in self.statement_spliter:
            code_parts = [sql_code]
            for sequence in self.statement_spliter:
                code_parts = itertools.chain.from_iterable(
                    part.split(sequence) for part in code_parts
                )

        else:
            # if ; is included split the file into sql statements
            # by using parse, we ensure not to split by escaped or commented
            # occurrences of ;

            for marker in self.statement_spliter:
                if marker != ";":
                    sql_code = sql_code.replace(marker, ";")

            code_parts = [
                "".join(token.value for token in statement)
                for statement in parse(sql_code)
            ]

        code_parts_handled = []
        for text in code_parts:
            code_parts_handled.append(self._handle_line_regex(text))

        for full_statement in code_parts_handled:
            cleaned_statement = (
                (
                    "".join(
                        token.value
                        for statement in parse(full_statement)
                        for token in statement
                        if token.ttype not in sqlparse.tokens.Comment
                    )
                )
                .strip()
                .strip(";")
            )

            # skip the statement unless it actually contains code.
            # spark.sql complains if you only give it comments
            if cleaned_statement:
                yield full_statement

    def _get_raw_contents(
        self,
        file_pattern: str,
        exclude_pattern: str = None,
    ):
        """Retrieve raw SQL file contents based on search patterns."""
        file_pattern = self._wildcard_string_to_regexp(file_pattern)
        if exclude_pattern is not None:
            exclude_pattern = self._wildcard_string_to_regexp(exclude_pattern)

        found = False
        for file_name in ir.contents(self.base_module):
            extension = Path(file_name).suffix
            if extension not in [".sql"]:
                continue

            if not re.match(file_pattern, Path(file_name).stem):
                continue

            if exclude_pattern is not None and re.search(
                exclude_pattern, Path(file_name).stem
            ):
                continue

            found = True
            with ir.path(self.base_module, file_name) as file_path:
                with open(file_path) as file:
                    yield file.read()

        if not found and not self.ignore_empty_folder:
            raise ValueError(f"No matching .sql files found in '{self.base_module}'")

    def execute_sql_file(
        self,
        file_pattern: str,
        exclude_pattern: str = None,
        replacements: Optional[Dict[str, str]] = None,
    ):
        """Execute SQL statements from files matching the given pattern."""
        executor = self.server or Spark.get()

        statement = None

        for statement in self.get_statements(
            file_pattern, exclude_pattern, replacements
        ):
            executor.sql(statement)

        if statement is None:
            print(
                f"WARNING: The SQL file pattern {file_pattern} "
                "resulted in zero statements being executed"
            )
