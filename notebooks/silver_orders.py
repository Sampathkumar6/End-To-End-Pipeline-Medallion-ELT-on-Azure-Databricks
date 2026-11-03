# Databricks notebook source
# MAGIC %md
# MAGIC #Data Reading

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("abfss://bronze@databrickssamproject2.dfs.core.windows.net/orders")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming

# COMMAND ----------

df = df.withColumnRenamed("_rescued_data","rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing_column

# COMMAND ----------

df = df.drop("rescued_data")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### date operations

# COMMAND ----------

# DBTITLE 1,Add year column
df = df.withColumn("year", year(col('order_date')))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### window functions

# COMMAND ----------

# DBTITLE 1,Rank orders by amount per year
df1 = df.withColumn("flag",row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Classes OOP

# COMMAND ----------

class windows:
    def dense_rank(self,df):
        df_dense_rank =df.withColumn("flag_dense_rank",dense_rank().over(Window.partitionBy("year").orderBy("total_amount")))
        return df_dense_rank
    def row_number(self,df):
        df_row_number = df.withColumn("flag_row_number",row_number().over(Window.partitionBy("year").orderBy("total_amount")))
        return df_row_number
    def rank(self,df):
        df_rank = df.withColumn("flag_rank",rank().over(Window.partitionBy("year").orderBy("total_amount")))
        return df_rank


# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.dense_rank(df_new)
display(df_result)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data write

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@databrickssamproject2.dfs.core.windows.net/orders")