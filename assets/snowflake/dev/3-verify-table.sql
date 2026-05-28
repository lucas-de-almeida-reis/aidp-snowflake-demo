-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  DEV — 3. Verify bronze in RAPPI_DEV                         ║
-- ╚══════════════════════════════════════════════════════════════════╝

USE DATABASE RAPPI_DEV;
USE SCHEMA   BRONZE;

SELECT CURRENT_DATABASE() AS db,
       CURRENT_SCHEMA()   AS schema,
       COUNT(*)           AS total_rows
FROM ORDER_DIMENSIONS;

SELECT COUNTRY_CODE, COUNT(*) AS n
FROM ORDER_DIMENSIONS
GROUP BY 1 ORDER BY 2 DESC;

SELECT ORDER_STATUS,
       COUNT(*) AS n,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM ORDER_DIMENSIONS
GROUP BY 1;

SELECT DATE_TRUNC('month', ORDER_CREATED_AT) AS mo,
       COUNT(*) AS n
FROM ORDER_DIMENSIONS
GROUP BY 1 ORDER BY 1;
