# Databricks notebook source
dbutils.fs.ls("/mnt/spractice")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`/mnt/spractice/online-retail-dataset.csv`
# MAGIC --direct reading with header is not supported

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW my_data_view
# MAGIC USING csv OPTIONS (
# MAGIC   path '/mnt/spractice/online-retail-dataset.csv',
# MAGIC   header 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM my_data_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using read_files function (Databricks Runtime 13.3 LTS and above)
# MAGIC SELECT * FROM read_files(
# MAGIC   '/mnt/spractice/online-retail-dataset.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   mode => 'FAILFAST')

# COMMAND ----------

spark.read.option("header", "true").csv(
    "/mnt/spractice/online-retail-dataset.csv"
).repartition(2).selectExpr("INSTR(Description, 'GLASS') >= 1 as is_glass").groupBy(
    "is_glass"
).count().collect()

# COMMAND ----------

df = spark.read.option("header", "true").csv(
    "/mnt/spractice/online-retail-dataset.csv"
).repartition(2).selectExpr("INSTR(Description, 'GLASS') >= 1 as is_glass").groupBy(
    "is_glass"
).count()
df.display()

# COMMAND ----------

print("Hello World")

# COMMAND ----------


