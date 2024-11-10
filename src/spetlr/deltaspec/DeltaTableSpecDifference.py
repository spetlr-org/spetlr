import copy
import dataclasses
import json
from dataclasses import _MISSING_TYPE, asdict, dataclass
from typing import Dict, List, Optional

from deprecated import deprecated
from pyspark.sql.types import StructField, StructType

from spetlr.deltaspec.DeltaTableSpecBase import DeltaTableSpecBase
from spetlr.deltaspec.exceptions import TableSpectNotEnforcable
from spetlr.deltaspec.helpers import TableName
from spetlr.schema_manager import SchemaManager


@deprecated("Class untested in current version of spetlr")
@dataclass
class DeltaTableSpecDifference:
    """This class contains two DeltaTableSpec, base and target.
    The class can make statements about the degree of agreement
    and can generate the ALTER TABLE statements that are
    needed to transform the base into the target.
    As a dataclass, the repr string for this class is able to
    restore the class object.
    """

    base: Optional[DeltaTableSpecBase]
    target: DeltaTableSpecBase

    def __post_init__(self):
        # decouple from the initializing objects
        self.base = (
            DeltaTableSpecBase(**asdict(self.base.copy())) if self.base else None
        )
        self.target = DeltaTableSpecBase(**asdict(self.target.copy()))

        # Only compare table names up to the highest level
        # that both DeltaTableSpec have information about.
        # so a nameless table can still compare equal to
        # another table. Also, unless both tables specify
        # the catalog, the catalog is ignored.

        if self.base:
            b_name = TableName.from_str(self.base.name)
            t_name = TableName.from_str(self.target.name)
            level = min(t_name.level(), b_name.level())
            self.base.name = str(b_name.to_level(level))
            self.target.name = str(t_name.to_level(level))

    def ignore(self, *keys: str):
        """Returns a new DeltaTableSpecDifference where the given set of
        keys are reset to default for both base and target."""

        # map each name to either a default factory object,
        # or a default object, or None
        # depending on what we have
        delta_spec_defaults = {
            f.name: (
                f.default_factory()
                if not isinstance(f.default_factory, _MISSING_TYPE)
                else f.default if not isinstance(f.default, _MISSING_TYPE) else None
            )
            for f in dataclasses.fields(self.target)
        }
        ignored_values = {key: delta_spec_defaults[key] for key in keys}
        new_base = (
            None
            if self.base is None
            else dataclasses.replace(self.base, **ignored_values)
        )
        new_target = dataclasses.replace(self.target, **ignored_values)
        return DeltaTableSpecDifference(base=new_base, target=new_target)

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
            and (self.base.cluster_by == self.target.cluster_by)
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

    def _schema_alter_statements(
        self,
        allow_columns_add=False,
        allow_columns_drop=False,
        allow_columns_type_change=False,
        errors_as_warnings=False,
        allow_columns_reorder=False,
    ) -> List[str]:
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
                if not allow_columns_add:
                    if errors_as_warnings:
                        print(f"WARNING: Would add column {key}. Continuing.")
                        continue
                    else:
                        raise TableSpectNotEnforcable(
                            f"use allow_columns_add to add {key}"
                        )

                fields_to_add.append(targetField)
                # adding the filed will fix the other field properties also
                continue

            # key is also in base
            base_field = base_fields[key]
            if base_field.dataType != targetField.dataType:
                if not allow_columns_type_change:
                    if errors_as_warnings:
                        print(
                            f"WARNING: Would change column {key}"
                            f" from {base_field.dataType}"
                            f" to {targetField.dataType}. Continuing."
                        )
                        continue
                    else:
                        raise TableSpectNotEnforcable(
                            f"set allow_columns_type_change to change {key}"
                        )

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
                if not allow_columns_drop:
                    if errors_as_warnings:
                        print(f"WARNING: Would drop column {key}. Continuing.")
                        continue
                    else:
                        raise TableSpectNotEnforcable(
                            f"use allow_columns_drop to drop {key}"
                        )
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
                    + SchemaManager().struct_to_sql(StructType(fields_to_add)).strip()
                    + ")"
                )
            else:
                statement += "S"
                statement += (
                    " (\n  "
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
        base_order_so_far = copy.copy(base_order)
        for field in fields_to_add:
            base_order_so_far.append(field.name)

        # We may not have added and dropped all necessary columns, so we need to act
        # on the subset of columns that exist in both.
        desired_order = [col for col in target_order if col in base_order_so_far]
        base_order_so_far = [col for col in base_order_so_far if col in desired_order]

        # using set operations would not have preserved the order.
        alter_order_statements = []

        if base_order_so_far[0] != desired_order[0]:
            # need to update the first column
            base_order_so_far.pop(base_order_so_far.index(desired_order[0]))
            base_order_so_far.insert(0, desired_order[0])
            alter_order_statements.append(
                f"ALTER TABLE {self.base.name} ALTER COLUMN {desired_order[0]} FIRST"
            )

        for i, key in enumerate(desired_order[1:], 1):
            if base_order_so_far[i] != key:
                base_order_so_far.pop(base_order_so_far.index(key))
                base_order_so_far.insert(i, key)
                alter_order_statements.append(
                    f"ALTER TABLE {self.base.name} ALTER COLUMN {key} AFTER "
                    + desired_order[i - 1]
                )

        if alter_order_statements:
            if not allow_columns_reorder:
                if errors_as_warnings:
                    print("WARNING: Would reorder columns. Continuing.")
                else:
                    raise TableSpectNotEnforcable("use allow_columns_reorder if needed")
            else:
                statements += alter_order_statements

        return statements

    def _alter_statements_for_new_location(self, allow_columns_add=False) -> List[str]:
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

        statements += DeltaTableSpecDifference(
            target=self.target, base=new_base
        ).alter_statements(allow_columns_add=allow_columns_add)
        return statements

    def alter_statements(
        self,
        allow_columns_add=False,
        allow_columns_drop=False,
        allow_columns_type_change=False,
        allow_columns_reorder=False,
        allow_name_change=False,
        allow_location_change=False,
        allow_table_create=True,
        errors_as_warnings=False,
    ) -> List[str]:
        """A list to alter statements that will ensure
        that what used to be the base table, becomes the target table"""
        target = self.target.fully_substituted()
        if self.base is None:
            if not (allow_table_create):
                if errors_as_warnings:
                    print(f"WARINING: Would create the table {target.name}")
                    return []
                else:
                    raise TableSpectNotEnforcable(
                        "use allow_table_create for creation " f"of {target.name}"
                    )
            else:
                return [target.get_sql_create()]

        full_diff = DeltaTableSpecDifference(
            target=target, base=self.base.fully_substituted()
        )

        base = full_diff.base
        target = full_diff.target

        if base.location != target.location:
            if allow_location_change:
                return full_diff._alter_statements_for_new_location(
                    allow_columns_add=allow_columns_add
                )
            else:
                if errors_as_warnings:
                    print(f"WARNING: Would change location of {base.name}. Continuing.")
                else:
                    raise TableSpectNotEnforcable(
                        "use allow_location_change if needed."
                    )

        statements = []

        # some table properties are needed to be able to drop columns.
        # therefore we need to set those first.
        statements += full_diff._tblproperties_alter_statements()

        statements += full_diff._schema_alter_statements(
            allow_columns_add=allow_columns_add,
            allow_columns_drop=allow_columns_drop,
            allow_columns_type_change=allow_columns_type_change,
            allow_columns_reorder=allow_columns_reorder,
            errors_as_warnings=errors_as_warnings,
        )

        if base.comment != target.comment:
            statements.append(
                f"""COMMENT ON TABLE {base.name} is {json.dumps(target.comment)}"""
            )

        # Change the name last so that we can still refer to the base name above this
        if base.name != target.name:
            if allow_name_change:
                statements.append(
                    f"""ALTER TABLE {base.name} RENAME TO {target.name}"""
                )
            else:
                if errors_as_warnings:
                    print(
                        f"WARNING: Would change name from {base.name} to {target.name}"
                    )
                else:
                    raise TableSpectNotEnforcable("set allow_name_change if needed.")

        return statements
