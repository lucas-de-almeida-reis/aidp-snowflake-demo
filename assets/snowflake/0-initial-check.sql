-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  0 — Bootstrap (run ONCE, creates both env databases)            ║
-- ║                                                                  ║
-- ║  Idempotent. After this runs, both RAPPI_DEV.BRONZE and          ║
-- ║  RAPPI_PROD.BRONZE exist on the same Snowflake account. Use      ║
-- ║  the per-env scripts under `dev/` and `prod/` after.             ║
-- ╚══════════════════════════════════════════════════════════════════╝

-- Warehouse hardening — applies to whichever WH the bronze pulls use.
ALTER WAREHOUSE COMPUTE_WH SET
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE;

-- Dev sandbox
CREATE DATABASE IF NOT EXISTS RAPPI_DEV;
CREATE SCHEMA   IF NOT EXISTS RAPPI_DEV.BRONZE;

-- Prod sandbox (same account; logical separation only)
CREATE DATABASE IF NOT EXISTS RAPPI_PROD;
CREATE SCHEMA   IF NOT EXISTS RAPPI_PROD.BRONZE;

-- Default warehouse for the worksheet
USE WAREHOUSE COMPUTE_WH;

-- Confirm
SELECT CURRENT_WAREHOUSE() AS warehouse,
       CURRENT_ROLE()      AS role;
SHOW DATABASES LIKE 'RAPPI_%';
