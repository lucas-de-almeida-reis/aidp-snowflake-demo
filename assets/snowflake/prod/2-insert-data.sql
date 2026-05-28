-- ╔══════════════════════════════════════════════════════════════════╗
-- ║  PROD — 2. Seed synthetic orders into RAPPI_PROD             ║
-- ║  10M rows — full demo                                            ║
-- ╚══════════════════════════════════════════════════════════════════╝

USE DATABASE RAPPI_PROD;
USE SCHEMA   BRONZE;
SET ROW_COUNT = 10000000;

INSERT INTO ORDER_DIMENSIONS
WITH base AS (
  SELECT
    seq4() + 1                                            AS rn,
    UNIFORM(0, 8, RANDOM())                               AS country_idx,
    UNIFORM(0, 8, RANDOM())                               AS vertical_idx,
    UNIFORM(0, 4, RANDOM())                               AS pay_idx,
    UNIFORM(1, 100, RANDOM())                             AS status_roll,
    UNIFORM(1, 100, RANDOM())                             AS fraud_roll,
    UNIFORM(1, 100, RANDOM())                             AS prime_roll,
    UNIFORM(1, 100, RANDOM())                             AS seg_roll,
    DATEADD(
      'second',
      MOD(ABS(RANDOM()), 86400 * DATEDIFF('day', '2015-08-01'::DATE, CURRENT_DATE()) + 1),
      '2015-08-01'::TIMESTAMP_NTZ
    )                                                     AS created_at
  FROM TABLE(GENERATOR(ROWCOUNT => $ROW_COUNT))
)
SELECT
  rn,
  UNIFORM(1, 50000000, RANDOM()),
  UNIFORM(1, 500000, RANDOM()),
  IFF(UNIFORM(1, 100, RANDOM()) <= 95, UNIFORM(1, 100000, RANDOM()), NULL),

  created_at,
  IFF(status_roll <= 90, DATEADD('minute', UNIFORM(15, 90, RANDOM()), created_at), NULL),
  IFF(status_roll BETWEEN 91 AND 95, DATEADD('minute', UNIFORM(1, 30, RANDOM()), created_at), NULL),
  IFF(status_roll <= 90, UNIFORM(15.0::FLOAT, 90.0::FLOAT, RANDOM()), NULL),

  ARRAY_CONSTRUCT('CO','MX','BR','AR','PE','CL','EC','UY','CR')[country_idx]::VARCHAR,
  ARRAY_CONSTRUCT('Bogota','CDMX','Sao Paulo','Buenos Aires','Lima','Santiago','Quito','Montevideo','San Jose')[country_idx]::VARCHAR,
  UNIFORM(-35.0::FLOAT, 25.0::FLOAT, RANDOM()),
  UNIFORM(-75.0::FLOAT, -35.0::FLOAT, RANDOM()),

  ARRAY_CONSTRUCT('RESTAURANTS','CPGS','E-COMMERCE','RAPPIFAVOR','SERVICES','ANTOJOS','RAPPICASH','RAPPI TRAVEL','RAPPI CARGO')[vertical_idx]::VARCHAR,

  ROUND(UNIFORM(5.0::FLOAT, 200.0::FLOAT, RANDOM()), 2),
  ROUND(UNIFORM(3.0::FLOAT, 180.0::FLOAT, RANDOM()), 2),
  ROUND(UNIFORM(0.5::FLOAT, 8.0::FLOAT, RANDOM()), 2),
  ROUND(UNIFORM(0.0::FLOAT, 3.0::FLOAT, RANDOM()), 2),
  ROUND(UNIFORM(0.0::FLOAT, 10.0::FLOAT, RANDOM()), 2),
  ROUND(UNIFORM(0.0::FLOAT, 15.0::FLOAT, RANDOM()), 2),
  ARRAY_CONSTRUCT('COP','MXN','BRL','ARS','PEN','CLP','USD','UYU','CRC')[country_idx]::VARCHAR,
  ARRAY_CONSTRUCT('credit_card','debit_card','cash','rappi_pay','rappi_cash')[pay_idx]::VARCHAR,

  CASE WHEN seg_roll <= 20 THEN 'new'
       WHEN seg_roll <= 80 THEN 'recurring'
       ELSE 'vip' END,
  UNIFORM(1, 500, RANDOM()),
  ARRAY_CONSTRUCT('organic','referral','google_ads','facebook_ads','partnership')[UNIFORM(0, 4, RANDOM())]::VARCHAR,

  prime_roll <= 25,
  IFF(prime_roll <= 25, ARRAY_CONSTRUCT('basic','plus','black')[UNIFORM(0,2,RANDOM())]::VARCHAR, NULL),

  ROUND(UNIFORM(0.0::FLOAT, 1.0::FLOAT, RANDOM()), 4),
  fraud_roll <= 2,
  IFF(fraud_roll <= 2, ARRAY_CONSTRUCT('velocity','geo_mismatch','card_blacklist','device_blacklist')[UNIFORM(0,3,RANDOM())]::VARCHAR, NULL),

  CASE WHEN status_roll <= 90 THEN 'delivered'
       WHEN status_roll <= 95 THEN 'cancelled'
       ELSE 'in_progress' END,
  IFF(status_roll BETWEEN 91 AND 95,
      ARRAY_CONSTRUCT('user_cancelled','restaurant_closed','courier_not_found','payment_failed')[UNIFORM(0,3,RANDOM())]::VARCHAR,
      NULL),
  ARRAY_CONSTRUCT('express','scheduled','priority')[UNIFORM(0,2,RANDOM())]::VARCHAR,
  UNIFORM(1, 100, RANDOM()) <= 60
FROM base;
