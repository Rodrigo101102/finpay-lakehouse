-- ==============================================================================
-- 01 · Crear Vistas Materializadas — Modelo Dimensional FinPay
-- ==============================================================================
-- Ejecutar UNA SOLA VEZ o ante cambios de esquema.
-- Las vistas se crean sobre tablas Gold y deben ejecutarse desde un SQL Warehouse.
--
-- Modelo dimensional:
-- - fact_transactions — tabla de hechos
-- - dim_merchant      — dimensión comercio
-- - dim_user          — dimensión usuario (PII bajo masking)
-- - dim_channel       — dimensión canal
-- - dim_date          — dimensión calendario
-- ==============================================================================

USE CATALOG fintech_finpay;

-- ------------------------------------------------------------------------------
-- dim_channel
-- ------------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_channel
COMMENT 'Dimensión de canal de origen de transacciones'
AS
SELECT
  ROW_NUMBER() OVER (ORDER BY channel) AS channel_sk,
  channel                              AS channel_id,
  CASE channel
    WHEN 'web' THEN 'Web'
    WHEN 'app' THEN 'App Móvil'
    WHEN 'pos' THEN 'POS Físico'
    ELSE 'Desconocido'
  END                                  AS channel_name,
  CASE channel
    WHEN 'web' THEN 'Digital'
    WHEN 'app' THEN 'Digital'
    WHEN 'pos' THEN 'Presencial'
    ELSE 'Otro'
  END                                  AS channel_type
FROM (
  SELECT DISTINCT channel
  FROM silver.transactions
  WHERE channel IS NOT NULL
);

-- ------------------------------------------------------------------------------
-- dim_date
-- ------------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_date
COMMENT 'Dimensión calendario con atributos de día, semana, mes, trimestre y año'
AS
SELECT
  CAST(DATE_FORMAT(d.fecha, 'yyyyMMdd') AS INT)  AS date_sk,
  d.fecha                                        AS full_date,
  YEAR(d.fecha)                                  AS anio,
  QUARTER(d.fecha)                               AS trimestre,
  MONTH(d.fecha)                                 AS mes,
  DATE_FORMAT(d.fecha, 'MMMM')                   AS mes_nombre,
  WEEKOFYEAR(d.fecha)                            AS semana_anio,
  DAYOFMONTH(d.fecha)                            AS dia_mes,
  DAYOFWEEK(d.fecha)                             AS dia_semana_num,
  DATE_FORMAT(d.fecha, 'EEEE')                   AS dia_semana_nombre,
  CASE WHEN DAYOFWEEK(d.fecha) IN (1, 7) THEN TRUE ELSE FALSE END AS es_fin_semana,
  CONCAT(YEAR(d.fecha), '-Q', QUARTER(d.fecha)) AS trimestre_label
FROM (
  SELECT EXPLODE(
    SEQUENCE(
      CAST('2020-01-01' AS DATE),
      CAST('2027-12-31' AS DATE),
      INTERVAL 1 DAY
    )
  ) AS fecha
) d;

-- ------------------------------------------------------------------------------
-- dim_merchant
-- ------------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_merchant
COMMENT 'Dimensión de comercios con categoría, país y estado de afiliación'
AS
SELECT
  m.merchant_id,
  m.merchant_name,
  m.category,
  m.country,
  m.affiliation_date,
  DATEDIFF(CURRENT_DATE(), m.affiliation_date) AS dias_afiliado,
  m.status,
  m.risk_level,
  m.es_activo,
  -- Perfil de transacciones (últimos 30 días)
  COALESCE(ms.transacciones_7d, 0)     AS transacciones_7d,
  COALESCE(ms.monto_total_7d, 0)       AS monto_total_7d,
  COALESCE(ms.tasa_reversa, 0)         AS tasa_reversa_7d,
  COALESCE(ms.score_riesgo, 0)         AS score_riesgo_actual,
  ms.ultima_transaccion_date
FROM silver.merchants m
LEFT JOIN gold.merchant_summary ms USING (merchant_id);

-- ------------------------------------------------------------------------------
-- dim_user
-- ------------------------------------------------------------------------------
-- PII bajo column masking heredado de silver.users
CREATE OR REPLACE MATERIALIZED VIEW gold.dim_user
COMMENT 'Dimensión de usuarios con segmento de riesgo y canal preferido — PII bajo masking'
AS
SELECT
  u.user_id,
  u.full_name,       -- masked para no-ingenieria
  u.document_id,     -- masked para no-ingenieria
  u.email,           -- masked para no-ingenieria
  u.phone,           -- masked para no-ingenieria
  u.country,
  u.segment,
  u.registration_date,
  DATEDIFF(CURRENT_DATE(), u.registration_date) AS dias_registro,
  -- Canal preferido: el más usado por el usuario
  cp.canal_preferido,
  -- Métricas de actividad
  COALESCE(ta.total_transacciones, 0) AS total_transacciones,
  COALESCE(ta.monto_total, 0)         AS monto_total_historico
FROM silver.users u
LEFT JOIN (
  SELECT
    user_id,
    FIRST_VALUE(channel) OVER (
      PARTITION BY user_id
      ORDER BY cnt DESC
    ) AS canal_preferido
  FROM (
    SELECT user_id, channel, COUNT(*) AS cnt
    FROM silver.transactions
    GROUP BY user_id, channel
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cnt DESC) = 1
) cp USING (user_id)
LEFT JOIN (
  SELECT
    user_id,
    COUNT(*)          AS total_transacciones,
    SUM(amount)       AS monto_total
  FROM silver.transactions
  GROUP BY user_id
) ta USING (user_id);

-- ------------------------------------------------------------------------------
-- fact_transactions
-- ------------------------------------------------------------------------------
CREATE OR REPLACE MATERIALIZED VIEW gold.fact_transactions
COMMENT 'Tabla de hechos de transacciones con métricas de monto, cantidad, tasa de reversa y score de riesgo'
AS
SELECT
  -- Claves de negocio
  t.transaction_id,
  t.user_id,
  t.merchant_id,
  t.channel,
  t.transaction_type,
  t.currency,
  t.status,
  t.reference_id,
  -- Claves subrogadas (join con dims)
  CAST(DATE_FORMAT(t.transaction_date, 'yyyyMMdd') AS INT) AS date_sk,
  dc.channel_sk,
  -- Métricas
  t.amount,
  -- Indicadores derivados
  CASE WHEN t.transaction_type = 'reversa'  THEN 1 ELSE 0 END AS es_reversa,
  CASE WHEN t.status = 'rechazado'          THEN 1 ELSE 0 END AS es_rechazada,
  CASE WHEN t.flag_reversa_sin_ref          THEN 1 ELSE 0 END AS es_reversa_sin_ref,
  -- Score de riesgo del comercio en la fecha
  COALESCE(k.score_riesgo, 0)    AS score_riesgo_comercio,
  COALESCE(k.tasa_reversa, 0)    AS tasa_reversa_comercio,
  -- Timestamp de ingesta
  t.transaction_date,
  t._ingestion_ts,
  t._pipeline_run_id
FROM silver.transactions t
LEFT JOIN gold.dim_channel dc
  ON t.channel = dc.channel_id
LEFT JOIN gold.transactions_kpis k
  ON  t.merchant_id       = k.merchant_id
  AND t.channel           = k.channel
  AND t.transaction_date  = k.transaction_date
  AND t.transaction_type  = k.transaction_type;

-- ------------------------------------------------------------------------------
-- Verificación
-- ------------------------------------------------------------------------------
SHOW MATERIALIZED VIEWS IN gold;

-- Contar filas en cada vista
SELECT 'fact_transactions' AS vista, COUNT(*) AS filas FROM gold.fact_transactions
UNION ALL
SELECT 'dim_merchant',  COUNT(*) FROM gold.dim_merchant
UNION ALL
SELECT 'dim_user',      COUNT(*) FROM gold.dim_user
UNION ALL
SELECT 'dim_channel',   COUNT(*) FROM gold.dim_channel
UNION ALL
SELECT 'dim_date',      COUNT(*) FROM gold.dim_date
ORDER BY vista;
