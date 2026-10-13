# Databricks notebook source
# MAGIC %md
# m_load_dept_txn (migrated from Informatica PowerCenter)
# MAGIC Parameterized by the `DEPT` widget (was mapping parameter `$$DEPT`).

# COMMAND ----------
from pyspark.sql import functions as F

DEPT = dbutils.widgets.get("DEPT")

# COMMAND ----------
# MAGIC %md ## SQ_DEPT_TXN  (Source Qualifier)

df = spark.read.table('bronze_dept_txn')
df = df.filter(F.col('DEPT_CODE') == DEPT)  # SELECT TXN_ID, ACCOUNT_NO, DEPT_CODE, PRODUCT, AMOUNT, CCY, 
df.createOrReplaceTempView('dept_txn')
# COMMAND ----------
# MAGIC %md ## LKP_CUSTOMER  (Lookup Procedure)

_lk = spark.read.table('customer_dim')
df = df.join(_lk, on='ACCOUNT_NO', how='left')  # DEPT_TXN.ACCOUNT_NO = CUSTOMER_DIM.ACCOUNT_NO
# COMMAND ----------
# MAGIC %md ## LKP_FX  (Lookup Procedure)

_lk = spark.read.table('fx_rates')
df = df.join(_lk, on='CCY', how='left')  # DEPT_TXN.CCY = FX_RATES.CCY
# COMMAND ----------
# MAGIC %md ## EXP_NORMALIZE  (Expression)

df = df.withColumn('DEPT', F.upper(F.col("DEPT_CODE")))
df = df.withColumn('AMOUNT_BASE', F.round((F.col("AMOUNT") * F.col("FX_RATE")), 2))
df = df.withColumn('IS_REV', F.when((F.col("STATUS") == "REVERSAL"), "Y").otherwise("N"))
# COMMAND ----------
# MAGIC %md ## FILT_ACTIVE  (Filter)

df = df.filter(((F.col("IS_REV") == "N") & (F.col("AMOUNT_BASE") != 0)))
# COMMAND ----------
# MAGIC %md ## AGG_DAILY  (Aggregator)

df = df.groupBy('CUSTOMER_ID', 'SEGMENT', 'DEPT', 'TXN_DATE').agg(F.sum(F.col("AMOUNT_BASE")).alias('AMOUNT_BASE'))
# COMMAND ----------
# MAGIC %md ## SEQ_TXN_KEY  (Sequence Generator)

df = df.withColumn('TXN_KEY', F.monotonically_increasing_id())
# COMMAND ----------
# MAGIC %md ## UPD_STRAT  (Update Strategy)

# Update Strategy -> Delta MERGE into the target
df.createOrReplaceTempView('upd')
# spark.sql('MERGE INTO fact_daily_customer t USING upd s ON ...')
