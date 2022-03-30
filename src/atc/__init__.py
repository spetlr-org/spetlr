"""
A common set of python libraries for DataBricks.
See https://github.com/atc-net/atc-dataplatform for details
"""
import importlib_metadata

__version__ = importlib_metadata.version("atc-dataplatform")

from atc import etl, functions, spark, sql  # noqa: F401
from atc.config_master.config_master import ConfigMaster  # noqa: F401
