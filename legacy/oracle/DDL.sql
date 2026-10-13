-- ============================================================================
-- Oracle source/target DDL for the bank feed consolidation (REP_BANK_PROD).
-- Migrated to Delta tables in Unity Catalog.
-- ============================================================================

-- Department feed landing table (loaded by the bank's file-mover from each dept)
CREATE TABLE BANK_STG.DEPT_TXN (
    TXN_ID       VARCHAR2(18),
    ACCOUNT_NO   VARCHAR2(12),
    DEPT_CODE    VARCHAR2(12),
    PRODUCT      VARCHAR2(20),
    AMOUNT       NUMBER(15,2),
    CCY          VARCHAR2(3),
    TXN_DATE     DATE,
    STATUS       VARCHAR2(10)
);

-- Customer dimension (lookup)
CREATE TABLE BANK_DW.CUSTOMER_DIM (
    ACCOUNT_NO   VARCHAR2(12) PRIMARY KEY,
    CUSTOMER_ID  VARCHAR2(18),
    SEGMENT      VARCHAR2(10)
);

-- FX rates (lookup)
CREATE TABLE BANK_DW.FX_RATES (
    CCY          VARCHAR2(3) PRIMARY KEY,
    FX_RATE      NUMBER(15,6)
);

-- Consolidated daily fact (target, updated by Update Strategy DD_UPDATE)
CREATE TABLE BANK_DW.FACT_DAILY_CUSTOMER (
    TXN_KEY      NUMBER(18) NOT NULL,
    CUSTOMER_ID  VARCHAR2(18),
    SEGMENT      VARCHAR2(10),
    DEPT         VARCHAR2(12),
    TXN_DATE     DATE,
    AMOUNT_BASE  NUMBER(18,2),
    PRIMARY KEY (TXN_KEY)
);

-- Gold summary (WF_GOLD_SUMMARY)
CREATE TABLE BANK_DW.GOLD_SEGMENT_DEPT_SUMMARY (
    SEGMENT      VARCHAR2(10),
    DEPT         VARCHAR2(12),
    TOTAL_BASE   NUMBER(18,2),
    TXN_COUNT    NUMBER(18)
);
