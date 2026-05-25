-- ============================================================
-- 02 · Refresh Vistas Materializadas — Modelo Dimensional FinPay
-- ============================================================
-- Ejecutado por finpay_semantic_job en cada ciclo DESPUÉS
-- de que el pipeline ETL complete exitosamente.
--
-- Este archivo contiene SOLO sentencias REFRESH MATERIALIZED VIEW.
-- El orden importa: primero las dimensiones, luego los hechos.
--
-- Compute requerido: SQL Warehouse (no cluster de propósito general)
-- ============================================================

USE CATALOG fintech_finpay;

-- 1. Dimensión canal (no depende de otras vistas)
REFRESH MATERIALIZED VIEW gold.dim_channel;

-- 2. Dimensión calendario (independiente)
REFRESH MATERIALIZED VIEW gold.dim_date;

-- 3. Dimensión comercio (depende de gold.merchant_summary)
REFRESH MATERIALIZED VIEW gold.dim_merchant;

-- 4. Dimensión usuario (depende de silver.transactions para canal preferido)
REFRESH MATERIALIZED VIEW gold.dim_user;

-- 5. Tabla de hechos (depende de todas las dims y Gold KPIs)
REFRESH MATERIALIZED VIEW gold.fact_transactions;

-- ============================================================
-- Verificación post-refresh
-- ============================================================
SELECT 'fact_transactions' AS vista, COUNT(*) AS filas, MAX(_ingestion_ts) AS ultima_ingestion FROM gold.fact_transactions
UNION ALL
SELECT 'dim_merchant',  COUNT(*), NULL FROM gold.dim_merchant
UNION ALL
SELECT 'dim_user',      COUNT(*), NULL FROM gold.dim_user
UNION ALL
SELECT 'dim_channel',   COUNT(*), NULL FROM gold.dim_channel
UNION ALL
SELECT 'dim_date',      COUNT(*), NULL FROM gold.dim_date
ORDER BY vista;
