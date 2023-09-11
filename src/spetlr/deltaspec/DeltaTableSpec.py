import json

import pyspark.sql.types
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.delta import DeltaHandle
from spetlr.deltaspec import DeltaTableSpecDifference
from spetlr.deltaspec.DeltaTableSpecBase import (
    DeltaTableSpecBase,
    _DEFAULT_blankedPropertyKeys,
)
from spetlr.deltaspec.exceptions import (
    InvalidSpecificationError,
    NoTableAtTarget,
    TableSpecNotReadable,
)
from spetlr.schema_manager import SchemaManager
from spetlr.schema_manager.spark_schema import get_schema
from spetlr.spark import Spark
from spetlr.sqlrepr.sql_types import repr_sql_types


class DeltaTableSpec(DeltaTableSpecBase):
    """This class represents a full specification for a delta table."""

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
        The name will be None since that is not stored in the data."""
        return cls.from_name(f"delta.`{location}`")

    @classmethod
    def from_tc(cls, id: str) -> "DeltaTableSpec":
        c = Configurator()
        # schema is required
        schema = get_schema(c.get(id, "schema")["sql"])
        init_args = dict(
            name=c.get(id, "name", default=None),
            location=c.get(id, "path", default=None),
            comment=c.get(id, "comment", default=None),
            schema=schema,
            options=c.get(id, "options", default={}),
            partitioned_by=c.get(id, "partitioned_by", default=[]),
            tblproperties=c.get(id, "tblproperties", default={}),
        )

        init_args = {k: v for k, v in init_args.items() if v}

        return DeltaTableSpec(**init_args)

    @classmethod
    def from_name(cls, in_name: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance,
        that describes the table of the given name."""
        spark = Spark.get()
        try:
            details = spark.sql(f"DESCRIBE DETAIL {in_name}").collect()[0].asDict()
        except AnalysisException as e:
            raise NoTableAtTarget(str(e))
        if details["format"] != "delta":
            raise InvalidSpecificationError("The table is not of delta format.")

        tblproperties = details["properties"]
        tblproperties["delta.minReaderVersion"] = str(details["minReaderVersion"])
        tblproperties["delta.minWriterVersion"] = str(details["minWriterVersion"])

        return DeltaTableSpec(
            name=details["name"],
            schema=spark.table(in_name).schema,
            partitioned_by=details["partitionColumns"],
            tblproperties=tblproperties,
            location=details["location"],
            comment=details["description"],
        )

    # String representations

    def __repr__(self):
        """Return a correct and minimal string,
         that can be evaluated as python code to return a DeltaTableSpec instance
        that will compare equal to the current instance."""
        parts = [
            (f"name={repr(self.name)}"),
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
            (
                f"blankedPropertyKeys={repr(self.blankedPropertyKeys)}"
                if self.blankedPropertyKeys != _DEFAULT_blankedPropertyKeys
                else ""
            ),
        ]

        return "DeltaTableSpec(" + (", ".join(p for p in parts if p)) + ")"

    def get_sql_create(self) -> str:
        """Returns a sql statement,
         that creates the table described by the current DeltaTableSpec instance.
        This method is guaranteed to be the inverse of the `.from_sql(sql)` constructor.
        """
        schema_str = SchemaManager().struct_to_sql(self.schema, formatted=True)
        sql = (
            "CREATE TABLE "
            + (self.name or f"delta.`{self.location}`")
            + f"\n(\n  {schema_str}\n)\n"
            + "USING DELTA\n"
        )

        if self.options:
            sub_parts = [
                json.dumps(k) + " = " + json.dumps(v)
                for k, v in sorted(self.options.items())
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
                f"  {json.dumps(k)} = {json.dumps(v)}"
                for k, v in sorted(self.tblproperties.items())
            ]
            sql += "TBLPROPERTIES (\n" + ",\n".join(sub_parts) + "\n)\n"

        return sql

    # identity manipulation
    def copy(self) -> "DeltaTableSpec":
        """Return an independent object that compares equal to this one."""
        globals = {
            k: v for k, v in vars(pyspark.sql.types).items() if not k.startswith("_")
        }
        globals.update(dict(DeltaTableSpec=DeltaTableSpec))
        return eval(repr(self), globals)

    def get_dh(self) -> DeltaHandle:
        full = self.fully_substituted()
        return DeltaHandle(name=full.name, location=full.location, data_format="delta")

    def compare_to(self, other: "DeltaTableSpec") -> DeltaTableSpecDifference:
        """Returns a DeltaTableSpecDifference
        of what this difference this object has with respect to the other."""
        return DeltaTableSpecDifference(base=other, target=self)

    def compare_to_location(self):
        """Returns a DeltaTableSpecDifference of self with respect to the disk."""
        unnamed = self.fully_substituted(name=None)
        return unnamed.compare_to_name()

    def compare_to_name(self):
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

    def make_storage_match(
        self,
        allow_columns_add=False,
        allow_columns_drop=False,
        allow_columns_type_change=False,
        allow_columns_reorder=False,
        allow_name_change=False,
        allow_location_change=False,
        allow_table_create=True,
        errors_as_warnings=False,
    ) -> None:
        """If storage is not exactly like the specification,
        change the storage to make it match."""
        diff = self.compare_to_name()

        if diff.is_different():
            spark = Spark.get()
            print(f"Now altering table {diff.target.name} to match specification:")
            for statement in diff.alter_statements(
                allow_columns_add=allow_columns_add,
                allow_columns_drop=allow_columns_drop,
                allow_columns_type_change=allow_columns_type_change,
                allow_columns_reorder=allow_columns_reorder,
                allow_name_change=allow_name_change,
                allow_location_change=allow_location_change,
                allow_table_create=allow_table_create,
                errors_as_warnings=errors_as_warnings,
            ):
                print(f"Executing SQL: {statement}")
                spark.sql(statement)

    def ensure_df_schema(self, df: DataFrame):
        # check if the df can be selected down into the schema of this table
        if not self.compare_to(
            DeltaTableSpec(schema=df.schema, name=None, location=self.location)
        ).is_readable():
            raise TableSpecNotReadable(
                "The data frame has an incompatible schema mismatch to this table."
            )
        return df.select(*self.schema.names)
