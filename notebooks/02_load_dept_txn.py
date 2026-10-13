# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — m_load_dept_txn (migrated from Informatica PowerCenter)
# MAGIC Parameterized by the `DEPT` widget (was mapping parameter `$$DEPT`).
# MAGIC Transformation chain: Source Qualifier → Lookups → Expression → Filter →
# MAGIC Aggregator → Sequence → Update Strategy (MERGE).

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_poc")
dbutils.widgets.text("schema", "dw")
dbutils.widgets.text("DEPT", "CARDS")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
DEPT = dbutils.widgets.get("DEPT")

# COMMAND ----------

# MAGIC %md ## SQ_DEPT_TXN (Source Qualifier)
df = (spark.table(f"{catalog}.{schema}.bronze_dept_txn")
      .filter(F.col("DEPT_CODE") == DEPT))

# COMMAND ----------

# MAGIC %md ## LKP_CUSTOMER + LKP_FX (Lookups)
cust = spark.table(f"{catalog}.{schema}.silver_customer_dim")
fx = spark.table(f"{catalog}.{schema}.silver_fx_rates")
df = df.join(cust, "ACCOUNT_NO", "left").join(fx, "CCY", "left")

# COMMAND ----------

# MAGIC %md ## EXP_NORMALIZE (Expression)
df = (df
      .withColumn("DEPT", F.upper(F.col("DEPT_CODE")))
      .withColumn("AMOUNT_BASE_CENTS",
                  F.round(F.col("AMOUNT") * F.coalesce(F.col("FX_RATE"), F.lit(0.0)) * 100).cast("long"))
      .withColumn("IS_REV", F.when(F.col("STATUS") == "REVERSAL", "Y").otherwise("N")))

# COMMAND ----------

# MAGIC %md ## FILT_ACTIVE (Filter)
df = df.filter((F.col("IS_REV") == "N") & (F.col("AMOUNT_BASE_CENTS") != 0))

# COMMAND ----------

# MAGIC %md ## AGG_DAILY (Aggregator)
agg = (df.groupBy("CUSTOMER_ID", "SEGMENT", "DEPT", "TXN_DATE")
         .agg(F.sum("AMOUNT_BASE_CENTS").alias("AMOUNT_BASE_CENTS"))
         .withColumn("AMOUNT_BASE", F.round(F.col("AMOUNT_BASE_CENTS") / 100, 2)))

# COMMAND ----------

# MAGIC %md ## SEQ_TXN_KEY + UPD_STRAT (Sequence Generator + Update Strategy → MERGE)
agg = agg.withColumn("TXN_KEY", F.monotonically_increasing_id() + 1)
agg.createOrReplaceTempView("dept_updates")
spark.sql(f"""
    MERGE INTO {catalog}.{schema}.fact_daily_customer AS t
    USING (SELECT TXN_KEY, CUSTOMER_ID, SEGMENT, DEPT, TXN_DATE, AMOUNT_BASE FROM dept_updates) AS s
    ON  t.CUSTOMER_ID = s.CUSTOMER_ID AND t.SEGMENT = s.SEGMENT
    AND t.DEPT = s.DEPT AND t.TXN_DATE = s.TXN_DATE
    WHEN MATCHED THEN UPDATE SET t.AMOUNT_BASE = s.AMOUNT_BASE
    WHEN NOT MATCHED THEN INSERT *
""")
print(f"dept {DEPT}: merged")
