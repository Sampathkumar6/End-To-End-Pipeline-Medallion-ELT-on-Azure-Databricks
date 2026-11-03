# Databricks notebook source
datasets = [
    {
        "file_name" : "orders"
    },
    {
        "file_name" : "products"
    },
    {
        "file_name" : "customers"
    }
]

# COMMAND ----------

dbutils.jobs.taskValues.set("output_datasets", datasets)