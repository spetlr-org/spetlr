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
        ignore_lines_starts_with: [str] = None,
    ):
        """Class to pre-treat sql statements and execute them.
        Replacement sequenced related to the Configurator will be inserted before
        execution.
        Giving not statement_spliter is equivalent to [";", "-- COMMAND ----------"].
        To disable splitting statements, please supply statement_spliter = None.
        Semicolon will be treated correctly when quoted or in comments.

        Default behavior supports spark, which will complain if given
        no actual sql code or on multiple statements."""

        self.base_module = base_module
        self.server = server
        if statement_spliter is self._DEFAULT:
            # the sequence "-- COMMAND ----------" is used in jupyter notebooks
            # and separates cells.
            # We treat it as another way to end a statement
            statement_spliter = [";", "-- COMMAND ----------"]
        self.statement_spliter = statement_spliter
        self.ignore_empty_folder = ignore_empty_folder

        # Be aware, that lines that should be ignored
        # can have some unforeseen consequences
        # If you want to remove LOCATION
        # and LOCATION is a name of a column
        # the column will be removed

        self.ignore_lines_starts_with = ignore_lines_starts_with

    def _wildcard_string_to_regexp(self, instr: str) -> str:
        # prepare file pattern:
        if instr.endswith(".sql"):
            instr = instr[:-4]

        instr = instr.replace("*", ".*")

        # the string end indicator will prevent us from matching the wrong files
        #  where a filename is also a prefix to another filename
        if not instr.endswith("$"):
            instr = instr + "$"

        return instr

    import re

    def _handle_line_regex(self, cleaned_statement: str) -> str:
        """
        Remove full lines if they match any regex pattern in `ignore_lines_regex`.
        Ensures semicolons are preserved when removing lines.
        """
        if not self.ignore_lines_regex:
            return cleaned_statement

        cleaned_lines = []
        for line in cleaned_statement.splitlines(
            keepends=True
        ):  # Keep original newlines
            stripped_for_check = line.lstrip()  # Preserve indentation for formatting

            # Check if the line matches any of the given regex patterns
            if any(
                re.match(pattern, stripped_for_check)
                for pattern in self.ignore_lines_regex
            ):
                if ";" in line:  # 🔥 If the line has a semicolon, preserve it
                    cleaned_lines.append(";\n")
            else:
                cleaned_lines.append(line)

        return "".join(cleaned_lines)  # Return cleaned SQL as a single string

    def _handle_line_starts_with(self, cleaned_statement: str) -> str:
        """
        Remove full lines if their first word matches any prefix in ignore_lines_starts_with.
        Ensures semicolons remain when removing lines that contain them.
        """
        if not self.ignore_lines_starts_with:
            return cleaned_statement  # No filtering needed

        cleaned_lines = []
        for line in cleaned_statement.splitlines(
            keepends=True
        ):  # Keep original newlines
            stripped_for_check = (
                line.lstrip()
            )  # Only used for checking, preserves indentation
            first_word = stripped_for_check.split(" ", 1)[0]  # Extract first word

            # 🔥 Preserve semicolons when removing a line
            if first_word in self.ignore_lines_starts_with:
                if ";" in line:
                    cleaned_lines.append(";\n")  # ✅ Keep semicolon on a separate line
            else:
                cleaned_lines.append(line)  # ✅ Keep untouched lines

        return "".join(cleaned_lines)  # Return as a single cleaned string

    def get_statements(
        self,
        file_pattern: str,
        exclude_pattern: str = None,
        replacements: Dict[str, str] = None,
    ):
        """
        NB: This sql parser can be challenged in parsing sql statements
        which do not use semicolon as a query separator only.
        """
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
        """Given the raw contents of a SQL file, break it down into statements
        and execute all substitutions."""
        if replacements is None:
            replacements = {}

        # Prepare the full set of replacements
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
            # If ";" is included, split the file into SQL statements
            # Using parse ensures we do not split by escaped or commented occurrences of ";"

            for marker in self.statement_spliter:
                if marker != ";":
                    sql_code = sql_code.replace(marker, ";")

            code_parts = [
                "".join(token.value for token in statement)
                for statement in parse(sql_code)
            ]

        for full_statement in code_parts:
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
            )  # Keep original stripping behavior

            # 🔥 Apply `_handle_line_starts_with` if ignore_lines_starts_with is set
            if self.ignore_lines_starts_with:
                cleaned_statement = self._handle_line_starts_with(cleaned_statement)

            # Skip the statement unless it actually contains code.
            # Spark SQL complains if you only give it comments
            if cleaned_statement:
                yield full_statement  # 🔥 Keeping `full_statement` as in the original code

    def _get_raw_contents(
        self,
        file_pattern: str,
        exclude_pattern: str = None,
    ):
        """Return the raw file contents to be parsed on to replacement parsing,
        based on the searched module, filepattern and exclude_pattern.
        Returns a generator of strings."""

        # prepare arguments:
        file_pattern = self._wildcard_string_to_regexp(file_pattern)
        if exclude_pattern is not None:
            exclude_pattern = self._wildcard_string_to_regexp(exclude_pattern)

        found = False
        # loop the module contents and find matching files
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
        """
        NB: This sql parser can be challenged in parsing sql statements
        which do not use semicolon as a query separator only.
        """

        executor = self.server or Spark.get()

        statement = None

        for statement in self.get_statements(
            file_pattern, exclude_pattern, replacements
        ):
            executor.sql(statement)

        if statement is None:
            print(
                f"WARNING: The sql file pattern {file_pattern} "
                "resulted in zero statements being executed"
            )
