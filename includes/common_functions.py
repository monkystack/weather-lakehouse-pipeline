"""
Common utility functions for data processing (Databricks environment)
    - perform upsert (merge) operation into a Delta table.
"""

def upsert_to_silver(df, table_name, merge_condition, exclude_cols=None):

    from delta.tables import DeltaTable

    # Default columns to exclude from update
    if exclude_cols is None:
        exclude_cols = ["city", "country", "event_time"]

    # Create table if not exists
    if not spark.catalog.tableExists(table_name):
        df.limit(0).write.format("delta").saveAsTable(table_name)

    target = DeltaTable.forName(spark, table_name)

    # Build update column mapping dynamically
    update_cols = {
        col: f"s.{col}"
        for col in df.columns
        if col not in exclude_cols
    }

    # Perform merge (upsert)
    (
        target.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdate(set=update_cols)
        .whenNotMatchedInsertAll()
        .execute()
    )