import itertools
import re
from importlib import resources as ir
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Optional, Union

from atc.configurator.configurator import Configurator
from atc.configurator.sql import sqlparse
from atc.configurator.sql.init_sqlparse import parse
from atc.schema_manager import SchemaManager
from atc.spark import Spark
from atc.sql import BaseExecutor


class SqlExecutor:
    _DEFAULT = object()

    def __init__(
        self,
        base_module: Union[str, ModuleType] = None,
        server: BaseExecutor = None,
        statement_spliter: Optional[List[str]] = _DEFAULT,
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

        # prepare arguments:
        file_pattern = self._wildcard_string_to_regexp(file_pattern)
        if exclude_pattern is not None:
            exclude_pattern = self._wildcard_string_to_regexp(exclude_pattern)
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

            with ir.path(self.base_module, file_name) as file_path:
                with open(file_path) as file:
                    conts = file.read()
                    sql_code = conts.format(**replacements)

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
                        )

                        # skip the statement unless it actually contains code.
                        # spark.sql complains if you only give it comments
                        if cleaned_statement:
                            yield full_statement

    def execute_sql_file(self, file_pattern: str, exclude_pattern: str = None):
        """
        NB: This sql parser can be challenged in parsing sql statements
        which do not use semicolon as a query separator only.
        """

        executor = self.server or Spark.get()

        for statement in self.get_statements(file_pattern, exclude_pattern):
            executor.sql(statement)
