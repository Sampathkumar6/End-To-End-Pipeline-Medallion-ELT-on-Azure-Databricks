# Databricks notebook source
# MAGIC %md
# MAGIC # FACT ORDERS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data reading

# COMMAND ----------

df = spark.sql("select * from databrick_project2.silver.silver_orders")

# COMMAND ----------

df_dimcus = spark.sql("select dim_cust_key, customer_id as dim_customer_id from databrick_project2.gold.dimcustomers")

df_dimpro = spark.sql("select product_id as dim_prod_key, product_id as dim_product_id from databrick_project2.gold.dimproducts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### replacing customer_id, product_id, with dim id's

# COMMAND ----------

# DBTITLE 1,Join orders with dimensions
df_fact  = df.join(df_dimcus,df['customer_id'] == df_dimcus['dim_customer_id'], how = 'left').join(df_dimpro,df['product_id'] == df_dimpro['dim_prod_key'], how = 'left')

# COMMAND ----------

df_fact_new = df_fact.drop("customer_id","product_id","dim_customer_id","dim_product_id")

# COMMAND ----------

df_fact_new.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### upsert condition

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Upsert fact orders
if spark.catalog.tableExists("databrick_project2.gold.factorders"):
    dlt_obj = DeltaTable.forName(spark, "databrick_project2.gold.factorders")
    dlt_obj.alias("t").merge(df_fact_new.alias("s"),"t.order_id = s.order_id and t.dim_cust_key = s.dim_cust_key and t.dim_prod_key = s.dim_prod_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
else:
    df_fact_new.write.format("delta").option("path","abfss://gold@databrickssamproject2.dfs.core.windows.net/factorders")\
    .saveAsTable("databrick_project2.gold.factorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databrick_project2.gold.factorders

# COMMAND ----------

