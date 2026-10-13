"""StructTypes derived from PowerCenter sources/targets."""
from pyspark.sql.types import (StructType, StructField, StringType,
                               LongType, DecimalType, DateType)

TYPE_MAP = {
    "string": "StringType()", "integer": "LongType()",
    "decimal": "DecimalType({p}, {s})", "date/time": "DateType()",
}

# SOURCE DEPT_TXN (Flat File)
DEPT_TXN_SCHEMA = StructType([
    StructField("TXN_ID", StringType(), True),
    StructField("ACCOUNT_NO", StringType(), True),
    StructField("DEPT_CODE", StringType(), True),
    StructField("PRODUCT", StringType(), True),
    StructField("AMOUNT", DecimalType(15, 2), True),
    StructField("CCY", StringType(), True),
    StructField("TXN_DATE", DateType(), True),
    StructField("STATUS", StringType(), True),
])

# SOURCE CUSTOMER_DIM (Oracle)
CUSTOMER_DIM_SCHEMA = StructType([
    StructField("ACCOUNT_NO", StringType(), True),
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("SEGMENT", StringType(), True),
])

# SOURCE FX_RATES (Oracle)
FX_RATES_SCHEMA = StructType([
    StructField("CCY", StringType(), True),
    StructField("FX_RATE", DecimalType(15, 6), True),
])

# TARGET FACT_DAILY_CUSTOMER (Oracle)
FACT_DAILY_CUSTOMER_SCHEMA = StructType([
    StructField("TXN_KEY", LongType(), True),
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("SEGMENT", StringType(), True),
    StructField("DEPT", StringType(), True),
    StructField("TXN_DATE", DateType(), True),
    StructField("AMOUNT_BASE", DecimalType(18, 2), True),
])
