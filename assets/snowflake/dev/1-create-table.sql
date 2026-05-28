-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  DEV — 1. Create the bronze table in RAPPI_DEV                 ║
-- ║  Self-contained: creates DB + schema if missing, then table.    ║
-- ╚══════════════════════════════════════════════════════════════════╝

CREATE DATABASE IF NOT EXISTS RAPPI_DEV;
USE DATABASE RAPPI_DEV;
CREATE SCHEMA IF NOT EXISTS BRONZE;
USE SCHEMA   BRONZE;

CREATE OR REPLACE TABLE ORDER_DIMENSIONS (
  -- Identity
  ORDER_ID                NUMBER(18,0)  NOT NULL,
  USER_ID                 NUMBER(18,0)  NOT NULL,
  STORE_ID                NUMBER(18,0)  NOT NULL,
  COURIER_ID              NUMBER(18,0),

  -- Times
  ORDER_CREATED_AT        TIMESTAMP_NTZ NOT NULL,
  ORDER_DELIVERED_AT      TIMESTAMP_NTZ,
  ORDER_CANCELLED_AT      TIMESTAMP_NTZ,
  DELIVERY_TIME_MINUTES   NUMBER(6,2),

  -- Geography
  COUNTRY_CODE            VARCHAR(2)    NOT NULL,
  CITY                    VARCHAR(64),
  LATITUDE                FLOAT,
  LONGITUDE               FLOAT,

  -- Verticals
  VERTICAL                VARCHAR(32)   NOT NULL,

  -- Financial
  TOTAL_AMOUNT            NUMBER(18,2)  NOT NULL,
  PRODUCT_AMOUNT          NUMBER(18,2)  NOT NULL,
  DELIVERY_FEE            NUMBER(18,2),
  SERVICE_FEE             NUMBER(18,2),
  TIP                     NUMBER(18,2),
  DISCOUNT                NUMBER(18,2),
  CURRENCY                VARCHAR(3)    NOT NULL,
  PAYMENT_METHOD          VARCHAR(32),

  -- CRM
  USER_SEGMENT            VARCHAR(32),
  ORDER_NUMBER_FOR_USER   NUMBER(8,0),
  ACQUISITION_CHANNEL     VARCHAR(32),

  -- Prime
  IS_PRIME_USER           BOOLEAN,
  PRIME_TIER              VARCHAR(16),

  -- Fraud
  FRAUD_SCORE             FLOAT,
  IS_FRAUDULENT           BOOLEAN,
  FRAUD_RULE_TRIGGERED    VARCHAR(64),

  -- Operations
  ORDER_STATUS            VARCHAR(16)   NOT NULL,
  CANCELLATION_REASON     VARCHAR(64),
  DELIVERY_TYPE           VARCHAR(16),
  IS_RECURRENT_ORDER      BOOLEAN
)
-- Cluster on the predicates Spark will push down (country + day).
-- Synthetic ORDER_CREATED_AT is random, so without clustering Snowflake
-- can't prune micro-partitions on date filters from the silver job.
CLUSTER BY (TO_DATE(ORDER_CREATED_AT), COUNTRY_CODE);
