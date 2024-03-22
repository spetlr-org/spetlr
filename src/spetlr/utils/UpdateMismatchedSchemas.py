from typing import List

from spetlr.configurator import Configurator
from spetlr.deltaspec import DeltaTableSpec


def get_table_ids_to_check():
    print("Remember to initialize the configurator first!")
    c = Configurator()
    table_ids_to_check = []
    for key in c._raw_resource_details.keys():
        if c.get(key, "update_on_delta_schema_mismatch", False):
            table_ids_to_check.append(key)
    return table_ids_to_check


def UpdateMismatchedSchemas(
    table_ids_to_check: List[str] = None,
):
    """For the following tables, if the production schema does not match
    the configured schema, update delta table and truncate.

    It is the responsibility of the developer to only add tables here where the
    code has the property that it can rebuild dropped tables.

    Tables can be flagged by using ´update_on_delta_schema_mismatch´
    as Configurator property.

    """

    if table_ids_to_check is None:
        table_ids_to_check = get_table_ids_to_check()

    for tbl_id in table_ids_to_check:
        tbl_spec = DeltaTableSpec.from_tc(tbl_id)
        if tbl_spec.compare_to_name().is_different():
            print(f"Updating table with id {tbl_id}")
            tbl_spec.make_storage_match(
                allow_columns_add=True,
                allow_columns_drop=True,
                allow_columns_type_change=True,
                allow_columns_reorder=True,
                allow_name_change=True,
                allow_location_change=True,
                allow_table_create=True,
                errors_as_warnings=True,
            )
            print(f"Truncating table with id {tbl_id}")
            tbl_spec.get_dh().truncate()
        else:
            print(f"No change for table with id {tbl_id}")
