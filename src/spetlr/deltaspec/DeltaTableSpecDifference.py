import copy
import dataclasses
import json
from typing import Dict, List, Optional

from pyspark.sql.types import StructField, StructType

from spetlr.deltaspec.DeltaDifferenceBase import DeltaDifferenceBase
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.deltaspec.exceptions import TableSpectNotEnforcable
from spetlr.deltaspec.helpers import TableName
from spetlr.schema_manager import SchemaManager


@dataclasses.dataclass
class DeltaTableSpecDifference(DeltaDifferenceBase):
    """This class contains two DeltaTableSpec, base and target.
    The class can make statements about the degree of agreement
    and can generate the ALTER TABLE statements that are
    needed to transform the base into the target.
    As a dataclass, the repr string for this class is able to
    restore the class object.
    """

    base: Optional[DeltaTableSpec]
    target: DeltaTableSpec

    def __post_init__(self):
        # decouple from the initializing objects
        self.base = self.base.copy() if self.base else None
        self.target = self.target.copy()

        # Only compare table names up to the highest level
        # that both DeltaTableSpec have information about.
        # so a nameless table can still compare equal to
        # another table. Also, unless both tables specify
        # the catalog, the catalog is ignored.

        if self.base:
            b_name = TableName.from_str(self.base.name)
            t_name = TableName.from_str(self.target.name)
            name_parts = 0
            if b_name.table and t_name.table:
                name_parts += 1
            if b_name.schema and t_name.schema:
                name_parts += 1
            if b_name.catalog and t_name.catalog:
                name_parts += 1
            self.base.name = str(b_name.to_level(name_parts))
            self.target.name = str(t_name.to_level(name_parts))

    def nullbase(self) -> bool:
        """is the comparison to a null base. Meaing there is no table."""
        return self.base is None

    def complete_match(self) -> bool:
        """A complete match = no difference"""
        if self.base is None:
            return False
        return self.base == self.target

    def is_different(self) -> bool:
        return not self.complete_match()

    def name_match(self):
        if self.base is None:
            return False
        return self.base.name == self.target.name

    def location_match(self):
        if self.base is None:
            return False
        return self.base.location == self.target.location

    def schema_match(self):
        if self.base is None:
            return False
        return self.base.schema == self.target.schema

    def metadata_match(self):
        if self.base is None:
            return False
        return (
            (self.base.options == self.target.options)
            and (self.base.partitioned_by == self.target.partitioned_by)
            and (self.base.comment == self.target.comment)
            and (self.base.tblproperties == self.target.tblproperties)
        )

    def is_readable(self) -> bool:
        """Returns true if the .read() and .append() methods can be executed
        on the target without throwing exceptions, under the assumption that
        the base represents to true state of data on disk."""
        if self.complete_match():
            return True

        if not self.location_match():
            return False

        # at this point we know that at least the name and location match
        if self.schema_match():
            return True

        # so the schemas are different.
        # The question remains if they are similar enough to allow reading.
        return self.schema_is_selectable()

    def schema_is_selectable(self):
        if self.base is None:
            return False
        # if differences are only in nuallability and comment,
        # we have no problem. This is handled implicitly.
        # If the specified schema is a subset of the storage schema,
        # then we can read without issue.

        # TODO: For now we do not support reading a
        #           subset of struct fields in the case of struct columns.
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

        # All specified fields are found with the correct dataType.
        # We can read the schema as-is (perhaps with a select statement
        # to ensure column capitalization and order)
        return True

    def _tblproperties_alter_statements(self) -> List[str]:
        """return that ALTER TABLE statements that will
        transform the tblproperties of the base table into the target"""
        statements = []
        if self.base.tblproperties == self.target.tblproperties:
            return statements

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
                f"ALTER TABLE {self.base.name} "
                f"""SET TBLPROPERTIES ({", ".join(parts)})"""
            )

        removed_properties = []
        for key in self.base.tblproperties.keys():
            if key not in self.target.tblproperties:
                removed_properties.append(key)
        if removed_properties:
            parts = [json.dumps(k) for k in removed_properties]
            statements.append(
                f"""ALTER TABLE {self.base.name} """
                f"""UNSET TBLPROPERTIES ({", ".join(parts)})"""
            )

        return statements

    def _schema_alter_statements(self, allow_new_columns=False) -> List[str]:
        if self.base.schema == self.target.schema:
            return []

        statements = []
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
                # adding the filed will fix the other field properties also
                continue

            # key is also in base
            base_field = base_fields[key]
            if base_field.dataType != targetField.dataType:
                fields_to_remove.append(key)
                fields_to_add.append(targetField)
                # adding the filed will fix the other field properties also
                continue

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
            if not allow_new_columns:
                raise TableSpectNotEnforcable(
                    "Cannot make the storage match without allowing new columns"
                )

        if fields_to_add:
            statement = f"ALTER TABLE {self.base.name} ADD COLUMN"
            if len(fields_to_add) == 1:
                statement += (
                    " ("
                    + SchemaManager().struct_to_sql(StructType(fields_to_add)).strip()
                    + ")"
                )
            else:
                statement += "S"
                statement += (
                    " (/n  "
                    + SchemaManager().struct_to_sql(
                        StructType(fields_to_add), formatted=True
                    )
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
                f"ALTER TABLE {self.base.name} ALTER COLUMN "
                f"{key} COMMENT {json.dumps(newComment)}"
            )

        # Finally address the possible change of order.

        intermediate_target_order = copy.copy(base_order)
        for field in fields_to_add:
            intermediate_target_order.append(field.name)

        # now the intermediate_target_order always contains
        # the same fields in the same order as the table for now.
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

        return statements

    def _alter_statements_for_new_location(self, allow_new_columns=False) -> List[str]:
        """In case we need to change location,
        the alter statements will consist of creating the new target.
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
            f"ALTER TABLE {self.base.name} "
            f"SET LOCATION {json.dumps(self.target.location)}"
        )
        # now to old base name points at the new location.
        # all that remains is to perhaps rename it
        new_base = nameless_target.fully_substituted(name=self.base.name)

        statements += self.target.compare_to(new_base).alter_statements(
            allow_new_columns=allow_new_columns
        )
        return statements

    def alter_statements(self, allow_new_columns=False) -> List[str]:
        """A list to alter statements that will ensure
        that what used to be the base table, becomes the target table"""
        if self.base is None:
            return [self.target.get_sql_create()]

        full_diff: DeltaTableSpecDifference = (
            self.target.fully_substituted().compare_to(self.base.fully_substituted())
        )

        base = full_diff.base
        target = full_diff.target

        if base.location != target.location:
            return full_diff._alter_statements_for_new_location(
                allow_new_columns=allow_new_columns
            )

        statements = []

        statements += full_diff._schema_alter_statements(
            allow_new_columns=allow_new_columns
        )

        if base.comment != target.comment:
            statements.append(
                f"""COMMENT ON TABLE {base.name} is {json.dumps(target.comment)}"""
            )

        statements += full_diff._tblproperties_alter_statements()

        # Change the name last so that we can still refer to the base name above this
        if base.name != target.name:
            statements.append(f"""ALTER TABLE {base.name} RENAME TO {target.name}""")

        return statements
