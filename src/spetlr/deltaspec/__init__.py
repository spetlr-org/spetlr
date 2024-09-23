"""
Specifications are based on this documentation:
https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using
"""

from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.deltaspec.DeltaTableSpecBase import DeltaTableSpecBase
from spetlr.deltaspec.DeltaTableSpecDifference import DeltaTableSpecDifference
from spetlr.deltaspec.exceptions import (
    InvalidSpecificationError,
    NoTableAtTarget,
    TableSpecNotReadable,
    TableSpecSchemaMismatch,
    TableSpectNotEnforcable,
)
