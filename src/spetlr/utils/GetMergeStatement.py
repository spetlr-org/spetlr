from typing import List


def GetMergeStatement(
    *,
    merge_statement_type: str,
    target_table_name: str,
    source_table_name: str,
    join_cols: List[str],
    insert_cols: List[str] = None,
    update_cols: List[str] = None,
    special_update_set: str = None,
) -> str:
    assert merge_statement_type in {"delta", "sql"}

    merge_sql_statement = (
        f"MERGE {'INTO ' if merge_statement_type == 'delta' else ''}"
        f"{target_table_name} AS target "
        f"USING {source_table_name} AS source "
        f"ON {' AND '.join(f'(source.{col} = target.{col})' for col in join_cols)} "
    )

    if update_cols and len(update_cols) > 0:
        merge_sql_statement += (
            "WHEN MATCHED THEN UPDATE "
            f"SET {', '.join(f'target.{col} = source.{col}' for col in update_cols)}"
        )

        if special_update_set:
            merge_sql_statement += f"{special_update_set} "
        else:
            merge_sql_statement += " "

    if insert_cols and len(insert_cols) > 0:
        merge_sql_statement += (
            "WHEN NOT MATCHED THEN "
            f"INSERT ({', '.join(insert_cols)}) "
            f"VALUES ({', '.join(f'source.{col}' for col in insert_cols)}) "
        )

    # End statement with ';'
    merge_sql_statement = merge_sql_statement.strip() + ";"

    return merge_sql_statement
