-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  0 — Initial check / bootstrap (run ONCE, both envs at the same time)
-- ║                                                                  ║
-- ║  Creates two databases on the SAME Snowflake account so dev and  ║
-- ║  prod are logically separated. The schema (BRONZE) and table     ║
-- ║  (ORDER_DIMENSIONS) names are identical inside each DB — only    ║
-- ║  the database name differs. That lets the AIDP config switch     ║
-- ║  envs by changing `sfDatabase` and nothing else.                 ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- 1) Warehouse hardening — applies to whichever WH the bronze pulls use.
ALTER WAREHOUSE COMPUTE_WH SET
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE;

-- 2) Dev sandbox
CREATE DATABASE IF NOT EXISTS RAPPI_DEV;
CREATE SCHEMA   IF NOT EXISTS RAPPI_DEV.BRONZE;

-- 3) Prod sandbox (same account; logical separation only)
CREATE DATABASE IF NOT EXISTS RAPPI_PROD;
CREATE SCHEMA   IF NOT EXISTS RAPPI_PROD.BRONZE;

-- 4) Default warehouse for the worksheet
USE WAREHOUSE COMPUTE_WH;

-- 5) Confirm
SELECT CURRENT_WAREHOUSE() AS warehouse,
       CURRENT_ROLE()      AS role;
SHOW DATABASES LIKE 'RAPPI_%';
