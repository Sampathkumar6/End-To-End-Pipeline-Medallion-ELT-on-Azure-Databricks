# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

df = spark.read.table("databrick_project2.bronze.regions")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing column

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickssamproject2.dfs.core.windows.net/regions")