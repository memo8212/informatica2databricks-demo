# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest department feeds to Bronze
# MAGIC Migrated from the PowerCenter session source step. In PowerCenter the flat
# MAGIC files were read by a Source Qualifier; here **Auto Loader** lands them into
# MAGIC Bronze, and the Oracle lookups become Silver tables.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_poc")
dbutils.widgets.text("schema", "dw")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing = f"/Volumes/{catalog}/landing/feeds"

# COMMAND ----------

# MAGIC %md ## Department transactions (Auto Loader)
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{landing}/_schema/dept_txn")
    .load(f"{landing}/dept_txn")
    .withColumn("_ingested_at", F.current_timestamp())
    .writeStream.format("delta").option("checkpointLocation", f"{landing}/_ckpt/dept_txn")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_dept_txn"))

# COMMAND ----------

# MAGIC %md ## Lookups (customer dimension, FX rates)
(spark.read.csv(f"{landing}/customer_dim", header=True, inferSchema=True)
     .write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_customer_dim"))
(spark.read.csv(f"{landing}/fx_rates", header=True, inferSchema=True)
     .write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.silver_fx_rates"))

print("bronze/silver lookups loaded")
