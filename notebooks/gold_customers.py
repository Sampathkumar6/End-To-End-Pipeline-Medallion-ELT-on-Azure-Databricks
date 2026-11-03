# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

df = spark.sql("select * from databrick_project2.silver.silver_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing duplicates

# COMMAND ----------

df = df.dropDuplicates(subset = ["customer_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dividing new vs old records

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# DBTITLE 1,Load existing gold records
if init_load_flag ==0:
    
    df_old = spark.sql('''select dim_cust_key , customer_id, create_date, update_date
                       from databrick_project2.gold.DimCustomers''')
else:
    df_old = spark.sql('''select 0 dim_cust_key , 0 customer_id, 0 create_date, 0 update_date
                       from databrick_project2.silver.silver_customers where 1=2''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming df_old

# COMMAND ----------

df_old = df_old.withColumnRenamed("dim_cust_key","old_dim_cust_key")\
            .withColumnRenamed("customer_id","old_customer_id")\
            .withColumnRenamed("create_date","old_create_date")\
            .withColumnRenamed("update_date","old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Applying join with old records

# COMMAND ----------

df_join = df.join(df_old,df.customer_id == df_old.old_customer_id, 'left')
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seperating new vs old records

# COMMAND ----------

df_new = df_join.filter(df_join.old_customer_id.isNull())

# COMMAND ----------

df_old = df_join.filter(df_join.old_customer_id.isNotNull())           

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparing df_old

# COMMAND ----------

#drop unwanteed
df_old = df_old.drop("old_customer_id","old_update_date")
#renaming
df_old = df_old.withColumnRenamed("old_create_date","create_date")
df_old = df_old.withColumnRenamed("old_dim_cust_key","dim_cust_key")
df_old = df_old.withColumn("create_date",to_timestamp(col("create_date")))
df_old = df_old.withColumn("update_date",current_timestamp())

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preparinf df_new

# COMMAND ----------

df_new = df_new.drop("old_customer_id","old_dim_cust_key","old_update_date", "old_create_date")

df_new = df_new.withColumn("update_date",current_timestamp())
df_new = df_new.withColumn("create_date",current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_old.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Surrogate key from 1

# COMMAND ----------

df_new = df_new.withColumn("dim_cust_key", monotonically_increasing_id()+lit(1))
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding max surrogate key

# COMMAND ----------

# DBTITLE 1,Get max surrogate key
if init_load_flag ==1:
    max_surrogate_key = 0
else:
    df_maxsur = spark.sql("select max(dim_cust_key) as max_surrogate_key from databrick_project2.gold.DimCustomers")

    #Converting df_maxsur to max_surrogate key variable
    max_surrogate_key = df_maxsur.collect()[0]['max_surrogate_key']

# COMMAND ----------

df_new = df_new.withColumn("dim_cust_key", lit(max_surrogate_key)+col("dim_cust_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union of df_old and df_new

# COMMAND ----------

df_final = df_new.unionByName(df_old)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD 1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if (spark.catalog.tableExists("databrick_project2.gold.DimCustomers")):
    
    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@databrickssamproject2.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.dim_cust_key = src.dim_cust_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_final.write.mode("overwrite")\
    .format("delta")\
    .option("path","abfss://gold@databrickssamproject2.dfs.core.windows.net/DimCustomers")\
    .saveAsTable("databrick_project2.gold.DimCustomers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrick_project2.gold.dimcustomers