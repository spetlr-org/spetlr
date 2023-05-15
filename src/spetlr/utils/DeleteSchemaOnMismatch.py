import json
from textwrap import dedent, indent
from typing import List

from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.delta.delta_handle import DeltaHandleInvalidName
from spetlr.spark import Spark
from spetlr.sql import SqlExecutor, SqlServer
from spetlr.utils import SqlCleanupSingleTestTables


def get_table_ids_to_check():
    print("Remember to initialize the configurator first!")
    c = Configurator()
    table_ids_to_check = []
    for key in c._raw_resource_details.keys():
        if c.table_property(key, "delete_on_delta_schema_mismatch", False):
            table_ids_to_check.append(key)
    return table_ids_to_check


def delete_mismatched_schemas(
    table_ids_to_check: List[str] = None,
    spark_executor: SqlExecutor = None,
    sqlserver_executor: SqlExecutor = None,
    delivery_server: SqlServer = None,
    sql_files_pattern: str = "*",
):
    """For the following tables, if the production schema does not match
    the configured schema, delete the table.

    It is the responsibility of the developer to only add tables here where the
    code has the property that it can rebuild dropped tables.

    Tables can be flagged by using ´delete_on_delta_schema_mismatch´ as Configurator property.

    """

    if spark_executor and sqlserver_executor:
        raise ValueError("Only provide a spark_executor or sqlserver_executor")

    if sqlserver_executor:
        is_delivery = True
    else:
        is_delivery = False

    if table_ids_to_check is None:
        table_ids_to_check = get_table_ids_to_check()

    executor = spark_executor or sqlserver_executor

    # first gather the configured schemas from the test databases
    print("Remember to initialize the configurator first!")
    configurator = Configurator()
    configurator.set_debug()
    executor.execute_sql_file(sql_files_pattern)

    schemas = {}
    for tbl_id in table_ids_to_check:
        try:
            schemas[tbl_id] = (
                delivery_server.read_table(tbl_id).schema
                if is_delivery
                else DeltaHandle.from_tc(tbl_id).read().schema
            )
        except DeltaHandleInvalidName:
            print(f"Not a valid delta handle {tbl_id}")
            continue

    # cleanup
    if is_delivery:
        SqlCleanupSingleTestTables(delivery_server).execute()
    else:
        test_dbs = Spark.get().sql(f"SHOW SCHEMAS LIKE '*__{configurator._unique_id}*'")
        for row in test_dbs.collect():
            Spark.get().sql(f"DROP DATABASE {row.databaseName} CASCADE")

    # now get the production schemas from the production tables
    affected_keys = []
    configurator.set_prod()
    for tbl_id in table_ids_to_check:
        try:
            prod_schema = (
                delivery_server.read_table(tbl_id).schema
                if is_delivery
                else DeltaHandle.from_tc(tbl_id).read().schema
            )
        except AnalysisException:
            print(f"Exception in reading production version of table id {tbl_id}")
            continue
        except DeltaHandleInvalidName:
            print(f"Not a valid delta handle {tbl_id}")
            continue
        if prod_schema != schemas[tbl_id]:
            print(f"Schema mismatch detected for table id {tbl_id}.")
            print(
                "  Production table schema:",
                indent(json.dumps(prod_schema.jsonValue(), indent=4), "  "),
            )
            print(
                "  Configured table schema:",
                indent(json.dumps(schemas[tbl_id].jsonValue(), indent=4), "  "),
            )
            if is_delivery:
                delivery_server.drop_table(tbl_id)
            else:
                DeltaHandle.from_tc(tbl_id).drop_and_delete()
            affected_keys.append(tbl_id)
