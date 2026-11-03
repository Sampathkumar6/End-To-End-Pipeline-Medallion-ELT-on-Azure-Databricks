# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations

# COMMAND ----------

my_rules = {
    "rule1": "product_id IS NOT NULL",
    "rule2": "product_name IS NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Table

# COMMAND ----------

# DBTITLE 1,DimProducts_stage from silver snapshot
# silver_products is overwritten (full snapshot) on every job run, so a plain stream fails.
# 1. Diff each silver snapshot by product_id into DimProducts_snapshot (no expectations allowed here).
dp.create_streaming_table(
    "DimProducts_snapshot",
    table_properties={"delta.enableChangeDataFeed": "true"}
)

dp.create_auto_cdc_from_snapshot_flow(
    target="DimProducts_snapshot",
    source="databrick_project2.silver.silver_products",
    keys=["product_id"],
    stored_as_scd_type=1
)

# 2. Stream the row-level changes of the snapshot table
@dp.temporary_view(name="DimProducts_snapshot_changes")
def DimProducts_snapshot_changes():
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table("DimProducts_snapshot")
        .filter("_change_type != 'update_preimage'")
    )

# 3. Apply those changes to DimProducts_stage with the not-null expectations
dp.create_streaming_table("DimProducts_stage", expect_all_or_drop=my_rules)

dp.create_auto_cdc_flow(
    target="DimProducts_stage",
    source="DimProducts_snapshot_changes",
    keys=["product_id"],
    sequence_by="_commit_version",
    apply_as_deletes="_change_type = 'delete'",
    except_column_list=["_change_type", "_commit_version", "_commit_timestamp"],
    stored_as_scd_type=1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming View

# COMMAND ----------

# DBTITLE 1,DimProducts_view
@dp.temporary_view(name="DimProducts_view")
def DimProducts_view():
    # the "Live." prefix is no longer needed; just use the dataset name
    return spark.read.table("DimProducts_stage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC pipeline

# COMMAND ----------

# DBTITLE 1,DimProducts SCD2
dp.create_streaming_table("DimProducts")

dp.create_auto_cdc_from_snapshot_flow(
    target="DimProducts",
    source="DimProducts_view",
    keys=["product_id"],
    stored_as_scd_type=2
)