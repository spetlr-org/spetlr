"""
A common set of python libraries for DataBricks.
See https://github.com/atc-net/atc-dataplatform for details
"""

from atc import etl, functions, spark, sql  # noqa: F401
from atc.configurator.configurator import Configurator  # noqa: F401

from .version import __version__  # noqa: F401

DEBUG = False


def dbg(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)
