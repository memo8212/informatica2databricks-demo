# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold summary (was WF_GOLD_SUMMARY)
# MAGIC Segment × department totals from the consolidated fact.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank_poc")
dbutils.widgets.text("schema", "dw")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

fact = spark.table(f"{catalog}.{schema}.fact_daily_customer")

gold = (fact.groupBy("SEGMENT", "DEPT")
            .agg(F.round(F.sum("AMOUNT_BASE"), 2).alias("TOTAL_BASE"),
                 F.count("*").alias("TXN_COUNT"))
            .orderBy("SEGMENT", "DEPT"))
gold.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.{schema}.gold_segment_dept_summary")
gold.show()
