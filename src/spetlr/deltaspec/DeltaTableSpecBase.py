import copy
import json
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Union

from deprecated import deprecated
from pyspark.sql.types import StructField, StructType

from spetlr import Configurator
from spetlr.deltaspec.exceptions import InvalidSpecificationError, TableSpecNotReadable
from spetlr.deltaspec.helpers import ensureStr, standard_databricks_location
from spetlr.schema_manager import SchemaManager

_DEFAULT = object()

_DEFAULT_minReaderVersion = 2
_DEFAULT_minWriterVersion = 5
_DEFAULT_blankedPropertyKeys = ["delta.columnMapping.maxColumnId"]


@deprecated("Class untested in current version of spetlr")
@dataclass
class DeltaTableSpecBase:
    """This class represents a full specification for a delta table."""

    name: Union[str, None]
    schema: StructType
    options: Dict[str, str] = field(default_factory=dict)
    partitioned_by: List[str] = field(default_factory=list)
    cluster_by: List[str] = field(default_factory=list)
    tblproperties: Dict[str, str] = field(default_factory=dict)
    location: Optional[str] = None
    comment: str = None

    # blanked properties will never be retained in the constructor
    blankedPropertyKeys: List[str] = field(default_factory=list)

    def __post_init__(self):
        """This method will be called automatically after instantiation and should
        not normally be called directly."""
        # The rationale here is that a name that contains a '{' will need
        # to be run through the configurator where keys are case-sensitive.
        # once a name is free of these, it may be used in comparisons to data
        # where it needs to be lower case

        self.name = ensureStr(self.name)
        self.location = ensureStr(self.location)

        if (
            (self.name and not self.name.startswith("delta.`"))  # a real name
            and self.location
            and self.location.startswith("dbfs:/user/hive/")
        ):
            # managed table locations start with this string.
            # we should treat this like an unspecified location
            self.location = None

        self.comment = ensureStr(self.comment)

        if self.name and "{" not in self.name:
            self.name = self.name.lower()

        for col in self.partitioned_by:
            if col not in self.schema.names:
                raise InvalidSpecificationError(
                    "Supply the partitioning columns in the schema."
                )

        for col in self.cluster_by:
            if col not in self.schema.names:
                raise InvalidSpecificationError(
                    "Supply the cluster columns in the schema."
                )

        self.location = standard_databricks_location(self.location)

        # Experiments have shown that the statement
        # ALTER TABLE she_test.tbl ALTER COLUMN a DROP NOT NULL
        # does not actually take effect on a table.
        # So we cannot work with not-nullable columns
        # in the future, if we want to generate these types
        # of alter statement, simply remove the following line.
        self.schema = self.remove_nullability(self.schema)

        # the functionality enabled by 'delta.columnMapping.mode' = 'name',
        # is needed for column manipulation and seems to go along with another
        # property "delta.columnMapping.maxColumnId": "5" which increases when
        # a column is added. it counts all the columns that were ever present
        # we should probably ignore that column in alter statements and comparisons.
        if not self.blankedPropertyKeys:
            self.blankedPropertyKeys = copy.copy(_DEFAULT_blankedPropertyKeys)
        for key in self.blankedPropertyKeys:
            if key in self.tblproperties:
                del self.tblproperties[key]

    def set_good_defaults(self):
        """
        These default values should be set on all tables
        that want to be able to drop and reorder columns.
        This function adds the following table properties unless they are already
        set to other values:
        - delta.minReaderVersion = "2"
        - delta.minWriterVersion = "5"
        - delta.columnMapping.mode = "name"
        """
        # We cannot set these as defaults in the init function because
        # when we let spark describe a table that does not have them we should not
        # just assume that they are set, or we would never apply them to a table
        # that really does not have them.

        # these two keys are special. They are set as table properties, but are
        # not read or handled as table properties.
        if "delta.minReaderVersion" not in self.tblproperties:
            self.tblproperties["delta.minReaderVersion"] = str(
                _DEFAULT_minReaderVersion
            )
        if "delta.minWriterVersion" not in self.tblproperties:
            self.tblproperties["delta.minWriterVersion"] = str(
                _DEFAULT_minWriterVersion
            )

        # This is a necessary condition for table alterations.
        # 'delta.columnMapping.mode' = 'name'
        if "delta.columnMapping.mode" in self.tblproperties:
            if self.tblproperties["delta.columnMapping.mode"] != "name":
                print(
                    f"WARNING: The table {self.name} is specified "
                    "with a property delta.columnMapping.mode != 'name'. "
                    "Expect table alteration commands to fail."
                )
        else:
            self.tblproperties["delta.columnMapping.mode"] = "name"

    def data_name(self) -> str:
        """Get a name that can be used in spark to access the underlying data."""
        c = Configurator()
        details = c.get_all_details()
        name = self.name.format(**details) if self.name else None
        location = self.location.format(**details) if self.location else None

        data_name = name or f"delta.`{location}`"
        if not data_name:
            raise TableSpecNotReadable("Name or location required to access data.")
        return data_name

    @classmethod
    def remove_nullability(self, schema: StructType) -> StructType:
        """Return a schema where the nullability of all fields is reset to default"""
        fields = []
        for f in schema.fields:
            fields.append(
                StructField(
                    name=f.name,
                    dataType=f.dataType,
                    nullable=True,
                    metadata={k: v for k, v in f.metadata.items() if v},
                )
            )
        return StructType(fields)

    # identity manipulation
    def copy(self) -> "DeltaTableSpecBase":
        """Return an independent object that compares equal to this one."""
        return copy.deepcopy(self)

    def fully_substituted(self, name=_DEFAULT) -> "DeltaTableSpecBase":
        """Return a new DeltaTableSpec
        where name and location have been completed via the Configurator."""
        parts = asdict(self)

        c = Configurator()
        details = c.get_all_details()
        parts["name"] = self.name.format(**details) if self.name else None
        parts["location"] = self.location.format(**details) if self.location else None

        # sometimes we want to override the name of the fully substituted object
        if name is not _DEFAULT:
            parts["name"] = name

        return DeltaTableSpecBase(**parts)

    def get_sql_create(self) -> str:
        """Returns a sql statement,
         that creates the table described by the current DeltaTableSpec instance.
        This method is guaranteed to be the inverse of the `.from_sql(sql)` constructor.
        """
        schema_str = SchemaManager().struct_to_sql(self.schema, formatted=True)
        self.data_name()  # raise if we cannot point to data

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

        if self.cluster_by:
            sql += f"CLUSTER BY ({', '.join(self.cluster_by)})\n"

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
