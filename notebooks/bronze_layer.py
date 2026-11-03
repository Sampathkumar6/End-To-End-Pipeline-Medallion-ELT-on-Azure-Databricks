# Databricks notebook source
# MAGIC %md
# MAGIC #Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic variable / parameter

# COMMAND ----------

# DBTITLE 1,Define file_name widget
dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### creting a schemalocation in checkpoint under bronze for orders data from source

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@databrickssamproject2.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://source@databrickssamproject2.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data writing

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@databrickssamproject2.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path", f"abfss://bronze@databrickssamproject2.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

df = spark.read.format("parquet").load(f"abfss://bronze@databrickssamproject2.dfs.core.windows.net/{p_file_name}")
display(df)