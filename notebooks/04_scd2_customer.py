# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — SCD2 customer dimension (migrated from m_scd2_customer)
# MAGIC Type-2 slowly changing dimension with Delta MERGE + Change Data Feed.
# MAGIC Close the current row when the segment changes (DD_UPDATE), then insert
# MAGIC the new version (DD_INSERT).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_poc")
dbutils.widgets.text("schema", "dw")
dbutils.widgets.text("BATCH_DATE", "2026-02-01")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
BATCH_DATE = dbutils.widgets.get("BATCH_DATE")

DIM = f"{catalog}.{schema}.dim_customer_scd2"
SRC = f"{catalog}.{schema}.silver_customer_dim"

# COMMAND ----------

# MAGIC %md ## Enable Change Data Feed on the dimension
spark.sql(f"ALTER TABLE {DIM} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md ## Source + lookup of the current dimension row (LKP_CURRENT)
src = spark.table(SRC)
src.createOrReplaceTempView("src_cust")

# COMMAND ----------

# MAGIC %md ## DD_UPDATE: close the current row when the segment changed
spark.sql(f"""
    MERGE INTO {DIM} AS t
    USING src_cust AS s
    ON t.ACCOUNT_NO = s.ACCOUNT_NO AND t.CURR_FLAG = 'Y' AND t.SEGMENT <> s.SEGMENT
    WHEN MATCHED THEN UPDATE SET t.CURR_FLAG = 'N', t.EFFECTIVE_TO = '{BATCH_DATE}'
""")

# COMMAND ----------

# MAGIC %md ## DD_INSERT: insert a new current version where none exists
spark.sql(f"""
    INSERT INTO {DIM}
    SELECT s.ACCOUNT_NO, s.CUSTOMER_ID, s.SEGMENT, '{BATCH_DATE}', NULL, 'Y'
    FROM   src_cust s
    LEFT JOIN {DIM} t ON t.ACCOUNT_NO = s.ACCOUNT_NO AND t.CURR_FLAG = 'Y'
    WHERE  t.ACCOUNT_NO IS NULL
""")

# COMMAND ----------

# MAGIC %md ## Read the Change Data Feed
(spark.read.format("delta")
      .option("readChangeFeed", "true")
      .option("startingVersion", 1)
      .table(DIM)
      .select("ACCOUNT_NO", "SEGMENT", "CURR_FLAG", "_change_type")
      .show(20, truncate=False))
