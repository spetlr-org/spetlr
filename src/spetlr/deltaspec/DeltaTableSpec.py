import copy
import dataclasses
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from pyspark.sql.types import StructType, StructField
from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.configurator.sql.parse_sql import parse_single_sql_statement
from spetlr.deltaspec.exceptions import InvalidSpecificationError, NoTableAtTarget
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

    # Non-trivial constructors
    @classmethod
    def from_sql(cls, sql: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance that describes the table that would be created
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
        """Return the DeltaTableSpec instance that describes the table found at the given storage location.
        The name will be None since that is not stored in the data.
        Currently, this does not support options or clustered_by"""
        return cls.from_name(f"delta.`{location}`")

    @classmethod
    def from_name(cls, in_name: str) -> "DeltaTableSpec":
        """Return the DeltaTableSpec instance that describes the table of the given name.
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
        """Return a correct and minimal string that can be evaluated as python code to return a DeltaTableSpec instance
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
        """Returns a sql statement that creates the table described by the current DeltaTableSpec instance.
        This method is guaranteed to be the inverse of the `.from_sql(sql)` constructor.
        """
        sql = (
            "CREATE TABLE "
            + (self.name or f"delta.`{self.location}`")
            + f"\n("
            + f"{SchemaManager().struct_to_sql(self.schema)}"
            + ")\n"
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
        return eval(repr(self))

    def fully_substituted(self, name=_DEFAULT) -> "DeltaTableSpec":
        """Return a new DeltaTableSpec where name and location have been completed via the Configurator."""
        result = copy.copy(self)

        c = Configurator()
        details = c.get_all_details()
        result.name = self.name.format(**details)
        result.location = self.location.format(**details)

        if name is not _DEFAULT:
            result.name = name

        return result

    def compare_to(self, other: "DeltaTableSpec"):
        """Returns a DeltaTableSpecDifference of what this difference this object has with respect to the other."""
        return DeltaTableSpecDifference(base=other, target=self)

    def compare_to_location(self):
        """Returns a DeltaTableSpecDifference of self with respect to the disk."""
        unnamed = self.fully_substituted(name=None)
        return unnamed.compare_to_storage()

    def compare_to_storage(self):
        """Returns a DeltaTableSpecDifference of self with respect to the catalog table of the same name."""
        full = self.fully_substituted()
        if full.name:
            onstorage = DeltaTableSpec.from_name(full.name)
        else:
            onstorage = DeltaTableSpec.from_path(full.location)
        return full.compare_to(onstorage)

    def is_readable(self):
        """Is the match to the specified location similar enough to allow reading as is?"""

        return self.compare_to_storage().is_readable()

    # If we can read, then we can also append.
    is_appendable = is_readable


@dataclasses.dataclass
class DeltaTableSpecDifference:
    base: DeltaTableSpec
    target: DeltaTableSpec

    def __bool__(self):
        """A complete match = no difference = false difference object"""
        return self.base != self.target

    def name_match(self):
        return self.base.name == self.target.name

    def location_match(self):
        return self.base.location == self.target.location

    def schema_match(self):
        return self.base.location == self.target.location

    def metadata_match(self):
        return (
            (self.base.options == self.target.options)
            and (self.base.partitioned_by == self.target.partitioned_by)
            and (self.base.comment == self.target.comment)
            and (self.base.tblproperties == self.target.tblproperties)
        )

    def is_readable(self):
        if not self:
            return True

        if not self.location_match():
            return False

        # at this point we know that at least the name and location match
        if self.schema_match():
            return True

        # so the schemas are different. The question remains if they are similar enough to allow reading.
        return self.schema_is_selectable()

    def schema_is_selectable(self):
        # if differences are only in nuallability and comment, we have no problem. This is handled implicitly.
        # If the specified schema is a subset of the storage schema, then we can read without issue.

        # TODO: For now we do not support reading a subset of struct fields in the case of struct columns.
        #       Each top level column has to match in type.

        # Check if the specified schema is a subset of the disk schema.
        storage_schema_dict: Dict[str, StructField] = {
            field.name: field for field in self.base.schema.fields
        }
        for field in self.target.schema.fields:
            # every field has to be in the storage, or we can stop
            if field.name not in storage_schema_dict:
                return False

            # every type has to match
            if storage_schema_dict[field.name].dataType != field.dataType:
                return False

        # All specified fields are found with the correct dataType. We can read the schema as-is
        # (perhaps with a select statement to ensure column capitalization and order)
        return True

    def _tblproperties_alter_statements(self) -> List[str]:
        """return that ALTER TABLE statements that will transform the tblproperties of the base tabel into the target"""
        statements = []

        # treat tblproperties
        if self.base.tblproperties != self.target.tblproperties:
            updated_properties = {}
            for key, target_value in self.target.tblproperties.items():
                if key in self.base.tblproperties:
                    if target_value == self.base.tblproperties[key]:
                        continue  # property is good
                    else:
                        updated_properties[key] = target_value
                else:  # key not in base properties
                    updated_properties[key] = target_value

            if updated_properties:
                parts = [
                    f"{json.dumps(k)} = {json.dumps(v)}"
                    for k, v in updated_properties.items()
                ]
                statements.append(
                    f"""ALTER TABLE {self.base.name} SET TBLPROPERTIES ({", ".join(parts)})"""
                )

            removed_properties = []
            for key in self.base.tblproperties.keys():
                if key not in self.target.tblproperties:
                    removed_properties.append(key)
            if removed_properties:
                parts = [json.dumps(k) for k in updated_properties.keys()]
                statements.append(
                    f"""ALTER TABLE {self.base.name} UNSET TBLPROPERTIES ({", ".join(parts)})"""
                )

        return statements

    def _schema_alter_statements(self) -> List[str]:
        statements = []
        if self.base.schema != self.target.schema:
            base_fields: Dict[str, StructField] = {
                field.name: field for field in self.base.schema.fields
            }
            base_order = [field.name for field in self.base.schema.fields]
            target_fields: Dict[str, StructField] = {
                field.name: field for field in self.target.schema.fields
            }
            target_order = [field.name for field in self.target.schema.fields]

            fields_to_add = []
            alter_comment = []  # (name,newValue)
            alter_nullability = []  # (name,newValue)
            fields_to_remove = []
            for key, targetField in target_fields.items():
                if key not in base_fields:
                    fields_to_add.append(targetField)
                    continue  # adding the filed will fix the other field properties also

                # key is also in base
                base_field = base_fields[key]
                if base_field.dataType != targetField.dataType:
                    fields_to_remove.append(key)
                    fields_to_add.append(targetField)
                    continue  # adding the filed will fix the other field properties also

                # key is also in base and dataType matches
                base_comment = base_field.metadata.get("comment", "")
                target_comment = targetField.metadata.get("comment", "")
                if base_comment != target_comment:
                    alter_comment.append((key, target_comment))

                if base_field.nullable != targetField.nullable:
                    alter_nullability.append((key, targetField.nullable))

            for key in base_fields.keys():
                if key not in target_fields:
                    fields_to_remove.append(key)

            # Now convert the accumulated changes to ALTER statements.
            if fields_to_remove:
                statement = f"ALTER TABLE {self.base.name} DROP COLUMN"
                if len(fields_to_remove) > 1:
                    statement += "S"
                statement += " (" + ", ".join(fields_to_remove) + ")"

                statements.append(statement)

            if fields_to_add:
                statement = f"ALTER TABLE {self.base.name} ADD COLUMN"
                if len(fields_to_add) == 1:
                    statement += (
                        " ("
                        + SchemaManager()
                        .struct_to_sql(StructType(fields_to_add))
                        .strip()
                        + ")"
                    )
                else:
                    statement += "S"
                    statement += (
                        " (/n"
                        + SchemaManager().struct_to_sql(StructType(fields_to_add))
                        + "\n)"
                    )

                statements.append(statement)

            for key, newValue in alter_nullability:
                statements.append(
                    f"ALTER TABLE {self.base.name} ALTER COLUMN {key} "
                    + ("SET" if newValue else "DROP")
                    + " NOT NULL"
                )

            for key, newComment in alter_comment:
                statements.append(
                    f"ALTER TABLE {self.base.name} ALTER COLUMN {key} COMMENT {json.dumps(newComment)}"
                )

            # Finally address the possible change of order.

            intermediate_target_order = copy.copy(base_order)
            for field in fields_to_add:
                intermediate_target_order.append(field.name)

            # now the intermediate_target_order always contains the same fields in the same order as the table for now.
            first = target_order[0]
            if intermediate_target_order[0] != first:
                intermediate_target_order.pop(intermediate_target_order.index(first))
                intermediate_target_order.insert(0, first)
                statements.append(
                    f"ALTER TABLE {self.base.name} ALTER COLUMN {first} FIRST"
                )

            for i, key in enumerate(target_order[1:], 1):
                if intermediate_target_order[i] != key:
                    intermediate_target_order.pop(intermediate_target_order.index(key))
                    intermediate_target_order.insert(i, key)
                    statements.append(
                        f"ALTER TABLE {self.base.name} ALTER COLUMN {key} AFTER "
                        + target_order[i - 1]
                    )

            # possible differences:
            # - different capitalization -- Not handled. Treated as new column
            # - different nullability -- Done
            # - different struct members (new, dropped) -- Not handled. Treated as new column
            # - different comment
            # - different order

            # Todo: FIX THEM ALL

        return statements

    def _alter_statements_for_new_location(self) -> List[str]:
        """In case we need to change location, the alter statements will consist of creating the new target.
        and then pointing the current table at it. The target will be empty."""

        if not self.target.location:
            raise NotImplementedError(
                "Converting an external to a managed table not implemented"
            )

        # Note: This will fail if the target is not empty

        # Create the target table, then point the current table at it
        nameless_target = self.target.fully_substituted(name=None)
        statements = [nameless_target.get_sql_create()]
        statements.append(
            f"ALTER TABLE {self.base.name} SET LOCATION {json.dumps(self.target.name)}"
        )
        # now to old base name points at the new location.
        # all that remains is to perhaps rename it
        new_base = nameless_target.fully_substituted(name=self.base.name)

        statements += self.target.compare_to(new_base).alter_table_statements()
        return statements

    def alter_table_statements(self) -> List[str]:
        """A list to alter statements that will ensure that what used to be the base table, becomes the target table"""
        base = self.base.fully_substituted()
        target = self.target.fully_substituted()
        full_diff = target.compare_to(base)

        if base.location != target.location:
            return full_diff._alter_statements_for_new_location()

        statements = []

        statements += full_diff._schema_alter_statements()

        if base.comment != target.comment:
            statements.append(
                f"""COMMENT ON {base.name} is {json.dumps(target.comment)}"""
            )

        statements += full_diff._tblproperties_alter_statements()

        # Change the name last so that we can still refer to the base name above this
        if base.name != target.name:
            statements.append(f"""ALTER TABLE {base.name} RENAME TO {target.name}""")

        return statements
