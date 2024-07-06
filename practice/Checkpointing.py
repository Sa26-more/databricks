# Databricks notebook source
# RDD Checkpointing Example
sc.setCheckpointDir("/mnt/spractice/checkpoint")
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd.checkpoint()
rdd.count()

# COMMAND ----------

# DataFrame Checkpointing Example
import tempfile
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
spark.sparkContext.setCheckpointDir("/mnt/spractice/checkpoint")
checkpointed_df = df.checkpoint(eager=True)

# COMMAND ----------

dbutils.fs.ls("/mnt/spractice/checkpoint/")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/spractice/checkpoint/b0b98e28-af69-4f85-adbe-109bda806e3e/"))
