# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@databrickssamproject2.dfs.core.windows.net/customers")

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming state column

# COMMAND ----------

df = df.withColumnRenamed("state","state_iso_code")

# COMMAND ----------

# MAGIC %md
# MAGIC ### adding derived domain name column from email

# COMMAND ----------

df = df.withColumn("Domain", split(col("email"),"@")[1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### addding derived total_customers per doamin column

# COMMAND ----------

df.groupBy("Domain").agg(count("customer_id").alias("toatl_customer")).sort("toatl_customer",ascending=False).display()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating derived full name

# COMMAND ----------

df = df.withColumn("Fullname",concat(col("first_name"),lit(" "),col("last_name")))

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df = df.drop("first_name","last_name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing delta file

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickssamproject2.dfs.core.windows.net/customers")