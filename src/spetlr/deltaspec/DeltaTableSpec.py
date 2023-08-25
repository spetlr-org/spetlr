import copy
import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.delta import DeltaHandle
from spetlr.deltaspec.DatabricksLocation import standard_databricks_location
from spetlr.deltaspec.DeltaTableDifference import DeltaTableDifference
from spetlr.deltaspec.exceptions import (
    InvalidSpecificationError,
    NoTableAtTarget,
    TableSpecNotReadable,
)
from spetlr.schema_manager import SchemaManager
from spetlr.schema_manager.spark_schema import get_schema
from spetlr.spark import Spark
from spetlr.sqlrepr.sql_types import repr_sql_types

_DEFAULT = object()


@dataclass
class DeltaTableSpec:
    """This class represents a full specification for a delta table."""

    name: str
    schema: StructType
    options: Dict[str, str] = field(default_factory=dict)
    partitioned_by: List[str] = field(default_factory=list)
    tblproperties: Dict[str, str] = field(default_factory=dict)
    location: Optional[str] = None
    comment: str = None
    # TODO: Clustered By

    def __init__(
        self,
        name: str,
        schema: StructType,
        options: Dict[str, str] = None,
        partitioned_by: List[str] = None,
        tblproperties: Dict[str, str] = None,
        location: Optional[str] = None,
        comment: str = None,
    ):
        # The rationale here is that a name that contains a '{' will need
        # to be run through the configurator where keys are case-sensitive.
        # once a name is free of these, it may be used in comparisons to data
        # where it needs to be lower case
        self.name = name.lower() if name and "{" not in name else name
        self.schema = schema
        self.options = options or dict()
        self.partitioned_by = partitioned_by or list()
        for col in self.partitioned_by:
            if col not in self.schema.names:
                raise InvalidSpecificationError(
                    "Supply the partitioning columns in the schema."
                )
        self.tblproperties = tblproperties or dict()
        self.location = standard_databricks_location(location)
        self.comment = comment

    # Non-trivial constructors
    @classmethod
    def from_sql(cls, sql: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance,
        that describes the table that would be created
        by the sql CREATE TABLE statement in the argument."""
        details = parse_single_sql_statement(sql)
        if details.get("format").lower() != "delta":
            raise InvalidSpecificationError(
                "The sql code is not a create table statement."
            )
        schema = get_schema(details["schema"]["sql"])
        init_args = dict(
            name=details.get("name"),
            location=details.get("path"),
            comment=details.get("comment"),
            schema=schema,
            options=details.get("options", {}),
            partitioned_by=details.get("partitioned_by", []),
            tblproperties=details.get("tblproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}

        return DeltaTableSpec(**init_args)

    @classmethod
    def from_path(cls, location: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance,
        that describes the table found at the given storage location.
        The name will be None since that is not stored in the data.
        Currently, this does not support options or clustered_by"""
        return cls.from_name(f"delta.`{location}`")

    @classmethod
    def from_tc(cls, id: str):
        c = Configurator()
        schema = get_schema(c.get(id, "schema")["sql"])
        init_args = dict(
            name=c.get(id, "name"),
            location=c.get(id, "path"),
            comment=c.get(id, "comment"),
            schema=schema,
            options=c.get(id, "options", {}),
            partitioned_by=c.get(id, "partitioned_by", []),
            tblproperties=c.get(id, "tblproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}

        return DeltaTableSpec(**init_args)

    @classmethod
    def from_name(cls, in_name: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance,
        that describes the table of the given name.
        Currently, this does not support options or clustered_by"""
        spark = Spark.get()
        try:
            details = spark.sql(f"DESCRIBE DETAIL {in_name}").collect()[0].asDict()
        except AnalysisException as e:
            raise NoTableAtTarget(str(e))
        if details["format"] != "delta":
            raise InvalidSpecificationError("The table is not of delta format.")
        name = details["name"]
        location = details["location"]
        partitioned_by = details["partitionColumns"]
        comment = details["description"]
        tblproperties = details["properties"]

        schema = spark.table(in_name).schema
        return DeltaTableSpec(
            name=name,
            schema=schema,
            partitioned_by=partitioned_by,
            tblproperties=tblproperties,
            location=location,
            comment=comment,
        )

    # String representations

    def __repr__(self):
        """Return a correct and minimal string,
         that can be evaluated as python code to return a DeltaTableSpec instance
        that will compare equal to the current instance."""
        parts = [
            (f"name={repr(self.name)}" if self.name else ""),
            f"schema={repr_sql_types(self.schema)}",
            (f"options={repr(self.options)}" if self.options else ""),
            (
                f"partitioned_by={repr(self.partitioned_by)}"
                if self.partitioned_by
                else ""
            ),
            (f"tblproperties={repr(self.tblproperties)}" if self.tblproperties else ""),
            (f"comment={repr(self.comment)}" if self.comment else ""),
            (f"location={repr(self.location)}" if self.location else ""),
        ]

        return "DeltaTableSpec(" + (", ".join(p for p in parts if p)) + ")"

    def get_sql_create(self) -> str:
        """Returns a sql statement,
         that creates the table described by the current DeltaTableSpec instance.
        This method is guaranteed to be the inverse of the `.from_sql(sql)` constructor.
        """
        sql = (
            "CREATE TABLE "
            + (self.name or f"delta.`{self.location}`")
            + f"\n({SchemaManager().struct_to_sql(self.schema)})\n"
            + "USING DELTA\n"
        )

        if self.options:
            sub_parts = [
                json.dumps(k) + " = " + json.dumps(v) for k, v in self.options.items()
            ]
            sql += f"OPTIONS ({', '.join(sub_parts)})\n"

        if self.partitioned_by:
            sql += f"PARTITIONED BY ({', '.join(self.partitioned_by)})\n"

        if self.location:
            sql += f"LOCATION {json.dumps(self.location)}\n"

        if self.comment:
            sql += f"COMMENT {json.dumps(self.comment)}\n"

        if self.tblproperties:
            sub_parts = [
                json.dumps(k) + " = " + json.dumps(v)
                for k, v in self.tblproperties.items()
            ]
            sql += f"TBLPROPERTIES ({', '.join(sub_parts)})\n"

        return sql

    # identity manipulation
    def copy(self) -> "DeltaTableSpec":
        """Return an independent object that compares equal to this one."""
        return eval(repr(self))

    def fully_substituted(self, name=_DEFAULT) -> "DeltaTableSpec":
        """Return a new DeltaTableSpec
        where name and location have been completed via the Configurator."""
        result = copy.copy(self)

        c = Configurator()
        details = c.get_all_details()
        result.name = self.name.format(**details)
        result.location = self.location.format(**details)

        if name is not _DEFAULT:
            result.name = name

        return result

    def compare_to(self, other: "DeltaTableSpec") -> DeltaTableDifference:
        """Returns a DeltaTableSpecDifference
        of what this difference this object has with respect to the other."""
        from spetlr.deltaspec._DeltaTableSpecDifference import DeltaTableSpecDifference

        return DeltaTableSpecDifference(base=other, target=self)

    def compare_to_location(self):
        """Returns a DeltaTableSpecDifference of self with respect to the disk."""
        unnamed = self.fully_substituted(name=None)
        return unnamed.compare_to_storage()

    def compare_to_storage(self):
        """Returns a DeltaTableSpecDifference of self
        with respect to the catalog table of the same name."""
        full = self.fully_substituted()
        try:
            if full.name:
                onstorage = DeltaTableSpec.from_name(full.name)
            else:
                onstorage = DeltaTableSpec.from_path(full.location)
        except NoTableAtTarget:
            onstorage = None
        return full.compare_to(onstorage)

    def is_readable(self):
        """Is the match to the specified location similar
        enough to allow reading as is?"""

        return self.compare_to_storage().is_readable()

    # If we can read, then we can also append.
    is_appendable = is_readable

    # Methods for compatibility with delta handles (non-streaming)

    def get_deltahandle(self) -> DeltaHandle:
        return DeltaHandle(name=self.name, location=self.location)

    def read(self) -> DataFrame:
        """Read table by path if location is given, otherwise from name."""
        diff = self.compare_to_storage()
        if not diff.is_readable():
            raise TableSpecNotReadable("Table not readable")
        if diff.schema_match():
            return self.get_deltahandle().read()
        else:
            return self.get_deltahandle().read().select(*self.schema.names)

    def write_or_append(
        self, df: DataFrame, mode: str, mergeSchema: bool = None
    ) -> None:
        assert mode in {"append", "overwrite"}
        diff = self.compare_to_storage()

        if mode == "append" and not diff.is_readable():
            raise TableSpecNotReadable(
                "Cannot append to a table of incompatible schema"
            )

        if diff:
            spark = Spark.get()
            for statement in diff.alter_table_statements():
                spark.sql(statement)

        return self.get_deltahandle().write_or_append(
            df=df, mode=mode, mergeSchema=mergeSchema
        )

    def make_storage_match(self) -> None:
        """If storage is not exactly like the specification,
        change the storage to make it match."""
        diff = self.compare_to_storage()
        if diff:
            spark = Spark.get()
            for statement in diff.alter_table_statements():
                spark.sql(statement)

    def overwrite(self, df: DataFrame, mergeSchema: bool = None) -> None:
        self.make_storage_match()
        return self.get_deltahandle().overwrite(df=df, mergeSchema=mergeSchema)

    def append(self, df: DataFrame, mergeSchema: bool = None) -> None:
        self.make_storage_match()
        return self.get_deltahandle().append(df=df, mergeSchema=mergeSchema)

    def upsert(
        self,
        df: DataFrame,
        join_cols: List[str],
    ) -> Union[DataFrame, None]:
        self.make_storage_match()
        return self.get_deltahandle().upsert(df=df, join_cols=join_cols)

    def delete_data(
        self, comparison_col: str, comparison_limit: Any, comparison_operator: str
    ) -> None:
        self.make_storage_match()
        return self.get_deltahandle().delete_data(
            comparison_col=comparison_col,
            comparison_limit=comparison_limit,
            comparison_operator=comparison_operator,
        )
