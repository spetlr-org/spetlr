import re
from importlib import resources as ir
from pathlib import Path
from types import ModuleType
from typing import Union

from atc.configurator.configurator import Configurator
from atc.spark import Spark
from atc.sql import BaseExecutor


class SqlExecutor:
    def __init__(
        self,
        base_module: Union[str, ModuleType] = None,
        server: BaseExecutor = None,
    ):
        self.base_module = base_module
        self.server = server

    def execute_sql_file(self, file_pattern: str, exclude_pattern: str = None):
        """
        NB: This sql parser can be challenged in parsing sql statements
        which do not use semicolon as a query separator only.
        """

        # prepare file pattern:
        if file_pattern.endswith(".sql"):
            file_pattern = file_pattern[:-4]

        file_pattern = file_pattern.replace("*", ".*")

        if exclude_pattern is not None:
            exclude_pattern = exclude_pattern.replace("*", ".*")

        replacements = Configurator().get_all_details()

        executor = self.server or Spark.get()

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
                    for statement in sql_code.split(";"):
                        cleaned_statement = ""
                        for line in statement.splitlines(keepends=True):
                            if line.lstrip().startswith("-- "):
                                continue
                            elif line.strip():
                                cleaned_statement += line
                        if cleaned_statement:
                            executor.sql(statement)
