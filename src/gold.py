"""
gold.py — Agregaciones Gold: KPIs de riesgo, tasas de reversa, score de riesgo y alertas.
FinPay Lakehouse · Azure Databricks
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from utils import compute_risk_score, CATALOG


# ============================================================================
# GOLD: KPIs de transacciones por comercio, canal y fecha
# ============================================================================

@dlt.table(
    name="transactions_kpis",
    schema=f"{CATALOG}.gold",
    comment="KPIs diarios de transacciones por comercio, canal y tipo — Gold layer",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def gold_transactions_kpis():
    txn = dlt.read(f"{CATALOG}.silver.transactions")

    # KPIs base por comercio + canal + fecha + tipo
    kpis = txn.groupBy(
        "merchant_id", "channel", "transaction_date", "transaction_type", "currency"
    ).agg(
        F.count("*").alias("total_transacciones"),
        F.sum("amount").cast(DecimalType(18, 2)).alias("monto_total"),
        F.avg("amount").cast(DecimalType(18, 2)).alias("monto_promedio"),
        F.min("amount").cast(DecimalType(18, 2)).alias("monto_minimo"),
        F.max("amount").cast(DecimalType(18, 2)).alias("monto_maximo"),
        F.countDistinct("user_id").alias("usuarios_unicos"),
        F.sum(F.when(F.col("status") == "aprobado",  1).otherwise(0)).alias("aprobadas"),
        F.sum(F.when(F.col("status") == "rechazado", 1).otherwise(0)).alias("rechazadas"),
        F.sum(F.when(F.col("status") == "pendiente", 1).otherwise(0)).alias("pendientes"),
        F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("reversas"),
        F.sum(F.when(F.col("flag_reversa_sin_ref"), 1).otherwise(0)).alias("reversas_sin_ref"),
        F.max("_ingestion_ts").alias("ultima_ingestion_ts")
    )

    # Tasas derivadas
    kpis = kpis.withColumn(
        "tasa_reversa",
        F.round(
            F.col("reversas") / F.nullif(F.col("total_transacciones"), F.lit(0)),
            4
        )
    ).withColumn(
        "tasa_rechazo",
        F.round(
            F.col("rechazadas") / F.nullif(F.col("total_transacciones"), F.lit(0)),
            4
        )
    ).withColumn(
        "tasa_aprobacion",
        F.round(
            F.col("aprobadas") / F.nullif(F.col("total_transacciones"), F.lit(0)),
            4
        )
    )

    # Score de riesgo calculado
    kpis = compute_risk_score(kpis)

    return kpis


# ============================================================================
# GOLD: Resumen de riesgo por comercio (ventana de 7 días)
# ============================================================================

@dlt.table(
    name="merchant_summary",
    schema=f"{CATALOG}.gold",
    comment="Resumen acumulado de riesgo por comercio — Gold layer",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    }
)
def gold_merchant_summary():
    txn = dlt.read(f"{CATALOG}.silver.transactions")
    mer = dlt.read(f"{CATALOG}.silver.merchants")

    # Ventana de últimos 7 días desde la fecha más reciente disponible
    max_date = txn.agg(F.max("transaction_date")).collect()[0][0]

    recent_txn = txn.filter(
        F.col("transaction_date") >= F.date_sub(F.lit(max_date), 7)
    )

    summary = recent_txn.groupBy("merchant_id").agg(
        F.count("*").alias("transacciones_7d"),
        F.sum("amount").cast(DecimalType(18, 2)).alias("monto_total_7d"),
        F.avg("amount").cast(DecimalType(18, 2)).alias("monto_promedio_7d"),
        F.sum(F.when(F.col("transaction_type") == "reversa", 1).otherwise(0)).alias("reversas_7d"),
        F.sum(F.when(F.col("status") == "rechazado", 1).otherwise(0)).alias("rechazadas_7d"),
        F.countDistinct("user_id").alias("usuarios_distintos_7d"),
        F.countDistinct("channel").alias("canales_usados"),
        F.max("transaction_date").alias("ultima_transaccion_date")
    ).withColumn(
        "tasa_reversa",
        F.round(F.col("reversas_7d") / F.nullif(F.col("transacciones_7d"), F.lit(0)), 4)
    ).withColumn(
        "tasa_rechazo",
        F.round(F.col("rechazadas_7d") / F.nullif(F.col("transacciones_7d"), F.lit(0)), 4)
    ).withColumn(
        "monto_promedio",   # alias requerido por compute_risk_score
        F.col("monto_promedio_7d")
    )

    summary = compute_risk_score(summary)

    # Enriquecer con datos del catálogo de comercios
    return (
        summary
        .join(
            mer.select("merchant_id", "merchant_name", "category", "country",
                       "status", "risk_level", "es_activo"),
            on="merchant_id",
            how="left"
        )
        .withColumn("_refreshed_at", F.current_timestamp())
    )


# ============================================================================
# GOLD: Alertas de fraude por comercio y canal
# ============================================================================

@dlt.table(
    name="fraud_alerts",
    schema=f"{CATALOG}.gold",
    comment="Alertas de fraude: comercios con score_riesgo > 60 o tasa_reversa > 0.30 — Gold layer",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "gold"
    }
)
def gold_fraud_alerts():
    """
    Genera alertas cuando:
    - score_riesgo > 60 (riesgo alto calculado)
    - tasa_reversa  > 0.30 (más del 30% de transacciones son reversas)
    - reversas_sin_ref > 0 (reversas sin transacción original — potencial fraude)
    """
    kpis = dlt.read(f"{CATALOG}.gold.transactions_kpis")

    alerts = kpis.filter(
        (F.col("score_riesgo") > 60) |
        (F.col("tasa_reversa") > 0.30) |
        (F.col("reversas_sin_ref") > 0)
    ).withColumn(
        "alerta_tipo",
        F.when(F.col("score_riesgo") > 60, "RIESGO_ALTO")
        .when(F.col("tasa_reversa") > 0.30, "ALTA_TASA_REVERSA")
        .when(F.col("reversas_sin_ref") > 0, "REVERSA_SIN_REFERENCIA")
        .otherwise("MULTIPLE")
    ).withColumn(
        "severidad",
        F.when(F.col("score_riesgo") > 80, "CRITICA")
        .when(F.col("score_riesgo") > 60, "ALTA")
        .when(F.col("tasa_reversa") > 0.30, "ALTA")
        .otherwise("MEDIA")
    ).withColumn(
        "alerta_ts", F.current_timestamp()
    )

    return alerts.select(
        "merchant_id", "channel", "transaction_date",
        "alerta_tipo", "severidad",
        "score_riesgo", "tasa_reversa", "tasa_rechazo",
        "total_transacciones", "reversas", "reversas_sin_ref",
        "monto_total", "alerta_ts"
    )
