# Databricks notebook source
print("Hello World")

# COMMAND ----------

spark.sparkContext.getConf().getAll()

# COMMAND ----------

spark.sql.adaptive.coalescePartitions.minPartitionNum
