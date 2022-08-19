from typing import List


def GetMergeStatement(
    *,
    target_table_name: str,
    source_table_name: str,
    join_cols: List[str],
    insert_cols: List[str],
    update_cols: List[str],
    special_update_set: str,
) -> str:
    merge_sql_statement = f"""
                MERGE INTO {target_table_name} AS target
                USING {source_table_name} AS source
                ON
                  {" AND ".join(f"(source.{col} = target.{col})" for col in join_cols)}
                WHEN MATCHED
                  THEN UPDATE -- update existing records
                    SET
                        {', '.join(f"target.{col} = source.{col}"
                                   for col in update_cols)}
                        {" "+special_update_set}
                WHEN NOT MATCHED
                  THEN INSERT -- insert new records
                    (
                        {', '.join(insert_cols)}
                    )
                  VALUES
                    (
                        {', '.join(f"source.{col}" for col in insert_cols)}
                    );
                """
    return merge_sql_statement
