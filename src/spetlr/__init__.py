"""
A python SPark ETL libRary (SPETLR) for Databricks.
See https://github.com/spetlr-org/spetlr for details
"""

from spetlr import etl, functions, spark, sql  # noqa: F401
from spetlr.configurator.configurator import Configurator  # noqa: F401

from .version import __version__  # noqa: F401

DEBUG = False


def dbg(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)
