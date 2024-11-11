import pyspark.sql.types
from deprecated import deprecated
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.delta import DeltaHandle
from spetlr.deltaspec.DeltaTableSpecBase import (
    DeltaTableSpecBase,
    _DEFAULT_blankedPropertyKeys,
)
from spetlr.deltaspec.DeltaTableSpecDifference import DeltaTableSpecDifference
from spetlr.deltaspec.exceptions import (
    InvalidSpecificationError,
    NoTableAtTarget,
    TableSpecNotReadable,
)
from spetlr.schema_manager.spark_schema import get_schema
from spetlr.spark import Spark
from spetlr.sqlrepr.sql_types import repr_sql_types


@deprecated("Class untested in current version of spetlr")
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
            cluster_by=details.get("cluster_by", []),
            tblproperties=details.get("tblproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}

        o = cls(**init_args)

        # objects initialized from parsed SQL should have these set
        # so that we define capable tables.
        o.set_good_defaults()

        return o

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
        item = c._get_item(id)
        schema = get_schema(item["schema"]["sql"])
        init_args = dict(
            name=item.get("name", None),
            location=item.get("path", None),
            comment=item.get("comment", None),
            schema=schema,
            options=item.get("options", {}),
            partitioned_by=item.get("partitioned_by", []),
            cluster_by=item.get("cluster_by", []),
            tblproperties=item.get("tblproperties", {}),
        )

        init_args = {k: v for k, v in init_args.items() if v}

        o = cls(**init_args)

        # objects initialized from parsed SQL should have these set
        # so that we define capable tables.
        o.set_good_defaults()

        return o

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

        return cls(
            name=details["name"],
            schema=spark.table(in_name).schema,
            partitioned_by=details["partitionColumns"],
            cluster_by=details["clusteringColumns"],
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
            (f"cluster_by={repr(self.cluster_by)}" if self.cluster_by else ""),
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
        return DeltaTableSpec.compare_to_name(self.fully_substituted(name=None))

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
        return DeltaTableSpecDifference(base=onstorage, target=full)

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

    def is_readable(self) -> bool:
        """Check if this delta table is readable when compared
        to the table of the same name currently in spark.
        Readability means that all columns in the schema of
        this specification also exist in table in spark.
        """
        return self.compare_to_name().is_readable()
