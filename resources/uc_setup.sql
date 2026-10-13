-- Unity Catalog setup for the bank feed consolidation POC.
-- Run once (needs CREATE CATALOG) before deploying the bundle.
-- Mirrors the PowerCenter folder BANK_ETL and its connections.

CREATE CATALOG IF NOT EXISTS bank_poc;
CREATE SCHEMA IF NOT EXISTS bank_poc.dw      COMMENT 'bank warehouse (migrated from BANK_DW)';
CREATE SCHEMA IF NOT EXISTS bank_poc.landing COMMENT 'department feed landing (Auto Loader)';

CREATE VOLUME IF NOT EXISTS bank_poc.landing.feeds
  COMMENT 'Department feed landing zone (was flat-file source connection)';

-- Bronze: department transaction feed
CREATE TABLE IF NOT EXISTS bank_poc.dw.bronze_dept_txn (
  TXN_ID STRING, ACCOUNT_NO STRING, DEPT_CODE STRING, PRODUCT STRING,
  AMOUNT DECIMAL(15,2), CCY STRING, TXN_DATE STRING, STATUS STRING,
  _ingested_at TIMESTAMP)
COMMENT 'Source Qualifier SQ_DEPT_TXN -> Bronze';

-- Silver lookups (were Oracle tables CUSTOMER_DIM / FX_RATES)
CREATE TABLE IF NOT EXISTS bank_poc.dw.silver_customer_dim
  (ACCOUNT_NO STRING, CUSTOMER_ID STRING, SEGMENT STRING)
  COMMENT 'Lookup CUSTOMER_DIM';
CREATE TABLE IF NOT EXISTS bank_poc.dw.silver_fx_rates
  (CCY STRING, FX_RATE DECIMAL(15,6))
  COMMENT 'Lookup FX_RATES';

-- Gold fact (target FACT_DAILY_CUSTOMER, Update Strategy DD_UPDATE -> MERGE)
CREATE TABLE IF NOT EXISTS bank_poc.dw.fact_daily_customer (
  TXN_KEY BIGINT NOT NULL, CUSTOMER_ID STRING, SEGMENT STRING, DEPT STRING,
  TXN_DATE DATE, AMOUNT_BASE DECIMAL(18,2))
COMMENT 'Target of m_load_dept_txn';

CREATE TABLE IF NOT EXISTS bank_poc.dw.gold_segment_dept_summary (
  SEGMENT STRING, DEPT STRING, TOTAL_BASE DECIMAL(18,2), TXN_COUNT BIGINT)
COMMENT 'Gold summary (WF_GOLD_SUMMARY)';

-- Consumers read Gold only.
-- GRANT SELECT ON TABLE bank_poc.dw.gold_segment_dept_summary TO `analysts`;

SELECT 'bank_poc setup complete' AS status;
