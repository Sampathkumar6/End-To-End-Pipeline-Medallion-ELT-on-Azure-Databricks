# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@databrickssamproject2.dfs.core.windows.net/products")
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### removing col

# COMMAND ----------

df = df.drop("_rescued_data")

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### user defind function

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databrick_project2.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.9
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, databrick_project2.bronze.discount_func(price) as Discounted_10 from products

# COMMAND ----------

df = df.withColumn("Discounted_price", expr("databrick_project2.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing Data

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickssamproject2.dfs.core.windows.net/products")