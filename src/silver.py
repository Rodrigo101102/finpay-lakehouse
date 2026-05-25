"""
silver.py — Limpieza, estandarización, deduplicación y enriquecimiento Silver.
Reto 2: Tabla de cuarentena para registros que no superan reglas de calidad.
FinPay Lakehouse · Azure Databricks
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

from utils import (
    add_audit_columns,
    clean_amount,
    deduplicate,
    normalize_string,
    parse_date_column,
    write_quarantine,
    CATALOG,
    VALID_CURRENCIES,
    VALID_CHANNELS,
    VALID_TX_TYPES,
    VALID_TX_STATUSES,
    VALID_COUNTRIES,
    VALID_SEGMENTS,
    VALID_RISK_LEVELS,
    VALID_MER_CATS,
    VALID_MER_STATUS,
)


# ============================================================================
# SILVER: TRANSACTIONS
# ============================================================================

@dlt.view(name="stg_transactions")
def stg_transactions():
    """Vista staging: limpieza base de transactions desde Bronze."""
    df = dlt.read_stream(f"{CATALOG}.bronze.transactions")

    # Normalizar strings categóricos
    df = normalize_string(df, "channel", "transaction_type", "status", "currency")

    # Limpiar y castear amount
    df = clean_amount(df, "amount")

    # Parsear fechas mixtas
    df = parse_date_column(df, "transaction_date")

    # Trim en IDs
    for col in ["transaction_id", "user_id", "merchant_id", "reference_id"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    # Estandarizar merchant_id: MCH00892 → MCH-00892
    df = df.withColumn(
        "merchant_id",
        F.regexp_replace(F.col("merchant_id"), r"^MCH(\d{5})$", "MCH-$1")
    )

    return df


@dlt.table(
    name="transactions",
    schema=f"{CATALOG}.silver",
    comment="Transacciones limpias y validadas — Silver layer",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
# ── 10 Expectations de calidad ──────────────────────────────────────────────
@dlt.expect_or_drop("transaction_id_not_null",
                    "transaction_id IS NOT NULL")
@dlt.expect_or_drop("transaction_id_format",
                    "transaction_id RLIKE '^TXN[0-9]{8}-[0-9]{5}$'")
@dlt.expect_or_drop("user_id_not_null",
                    "user_id IS NOT NULL")
@dlt.expect_or_drop("user_id_format",
                    "user_id RLIKE '^USR-?[0-9]{6}$'")
@dlt.expect_or_drop("merchant_id_not_null",
                    "merchant_id IS NOT NULL")
@dlt.expect_or_drop("merchant_id_format",
                    "merchant_id RLIKE '^MCH-[0-9]{5}$'")
@dlt.expect_or_drop("amount_positive",
                    "amount > 0")
@dlt.expect_or_drop("transaction_date_not_null",
                    "transaction_date IS NOT NULL")
@dlt.expect_or_drop("currency_valid",
                    f"currency IN ({','.join(repr(c) for c in VALID_CURRENCIES)})")
@dlt.expect_or_drop("channel_valid",
                    f"channel IN ({','.join(repr(c) for c in VALID_CHANNELS)})")
def silver_transactions():
    df = dlt.read_stream("stg_transactions")

    # Regla de negocio: reference_id obligatorio en reversas
    df = df.withColumn(
        "reference_id",
        F.when(
            (F.col("transaction_type") == "reversa") & F.col("reference_id").isNull(),
            F.lit("MISSING_REF")
        ).otherwise(F.col("reference_id"))
    )

    # Columna de alerta: reversa sin referencia válida
    df = df.withColumn(
        "flag_reversa_sin_ref",
        (F.col("transaction_type") == "reversa") & (F.col("reference_id") == "MISSING_REF")
    )

    # Columna enriquecida: hora de transacción (para análisis intraday)
    df = df.withColumn("transaction_hour", F.hour(F.col("_ingestion_ts")))

    # Deduplicar por PK
    df = deduplicate(df, ["transaction_id"])

    return df.select(
        "transaction_id", "user_id", "merchant_id", "channel",
        "transaction_type", "amount", "currency", "transaction_date",
        "status", "reference_id", "flag_reversa_sin_ref", "transaction_hour",
        "_source_name", "_source_file", "_ingestion_ts", "_pipeline_run_id"
    )


# ── Tabla de cuarentena: registros rechazados de transactions ────────────────
@dlt.table(
    name="transactions_quarantine_feed",
    schema=f"{CATALOG}.silver",
    comment="Registros de transactions rechazados por expectations — para cuarentena",
    table_properties={"quality": "quarantine"}
)
@dlt.expect_all_or_drop({
    "transaction_id_not_null": "transaction_id IS NOT NULL",
    "transaction_id_format":   "transaction_id RLIKE '^TXN[0-9]{8}-[0-9]{5}$'",
    "amount_positive":         "amount > 0",
    "transaction_date_not_null": "transaction_date IS NOT NULL",
    "currency_valid":          f"currency IN ({','.join(repr(c) for c in VALID_CURRENCIES)})",
    "channel_valid":           f"channel IN ({','.join(repr(c) for c in VALID_CHANNELS)})",
})
def transactions_quarantine_feed():
    """
    Captura registros que NO pasan las validaciones.
    DLT drop-mode descarta en silver_transactions; aquí los retenemos
    añadiendo motivo de rechazo para la tabla silver.quarantine.
    """
    df = dlt.read_stream("stg_transactions")

    # Marcar registros inválidos con motivo de rechazo
    df = df.withColumn(
        "rejection_reason",
        F.when(F.col("transaction_id").isNull(), "transaction_id nulo")
        .when(~F.col("transaction_id").rlike(r"^TXN[0-9]{8}-[0-9]{5}$"), "transaction_id formato inválido")
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), "amount inválido o no positivo")
        .when(F.col("transaction_date").isNull(), "transaction_date nula o formato inválido")
        .when(~F.col("currency").isin(list(VALID_CURRENCIES)), "currency inválida")
        .when(~F.col("channel").isin(list(VALID_CHANNELS)), "channel inválido")
        .otherwise(None)
    ).filter(F.col("rejection_reason").isNotNull())

    return df.withColumn("source_name", F.lit("transactions")) \
             .withColumn("rejected_field", F.lit("múltiples")) \
             .withColumn("original_record", F.to_json(F.struct("*"))) \
             .withColumn("processed_at", F.current_timestamp()) \
             .select(
                 "source_name", "rejection_reason", "rejected_field",
                 "original_record", "processed_at", "_pipeline_run_id"
             )


# ============================================================================
# SILVER: MERCHANTS
# ============================================================================

@dlt.table(
    name="merchants",
    schema=f"{CATALOG}.silver",
    comment="Catálogo de comercios limpio y validado — Silver layer",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)
@dlt.expect_or_drop("merchant_id_not_null",   "merchant_id IS NOT NULL")
@dlt.expect_or_drop("merchant_name_not_null", "merchant_name IS NOT NULL")
@dlt.expect_or_drop("country_valid",
                    f"country IN ({','.join(repr(c) for c in VALID_COUNTRIES)})")
@dlt.expect_or_drop("affiliation_date_not_null", "affiliation_date IS NOT NULL")
def silver_merchants():
    df = dlt.read_stream(f"{CATALOG}.bronze.merchants")

    # Estandarizar merchant_id: MCH00892 → MCH-00892
    df = df.withColumn(
        "merchant_id",
        F.regexp_replace(F.trim(F.col("merchant_id")), r"^MCH(\d{5})$", "MCH-$1")
    )

    # Normalizar strings
    df = normalize_string(df, "category", "status", "risk_level", "country")
    df = df.withColumn("merchant_name", F.trim(F.col("merchant_name")))

    # Parsear fecha de afiliación
    df = parse_date_column(df, "affiliation_date")

    # Enriquecer: flag comercio activo
    df = df.withColumn("es_activo", F.col("status") == F.lit("activo"))

    # Deduplicar
    df = deduplicate(df, ["merchant_id"])

    return df.select(
        "merchant_id", "merchant_name", "category", "country",
        "affiliation_date", "status", "risk_level", "es_activo",
        "_source_name", "_source_file", "_ingestion_ts", "_pipeline_run_id"
    )


# ============================================================================
# SILVER: USERS  (con PII — column masking aplicado a nivel de tabla en setup)
# ============================================================================

@dlt.table(
    name="users",
    schema=f"{CATALOG}.silver",
    comment="Usuarios FinPay limpios — PII bajo column masking y RLS (ver 00_setup)",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "silver"
    }
)
@dlt.expect_or_drop("user_id_not_null",   "user_id IS NOT NULL")
@dlt.expect_or_drop("user_id_format",     "user_id RLIKE '^USR-?[0-9]{6}$'")
@dlt.expect_or_drop("email_not_null",     "email IS NOT NULL")
@dlt.expect_or_drop("country_valid_usr",
                    f"country IN ({','.join(repr(c) for c in VALID_COUNTRIES)})")
@dlt.expect_or_drop("registration_date_not_null", "registration_date IS NOT NULL")
def silver_users():
    df = dlt.read_stream(f"{CATALOG}.bronze.users")

    # Normalizar user_id: USR001234 → USR-001234
    df = df.withColumn(
        "user_id",
        F.regexp_replace(F.trim(F.col("user_id")), r"^USR(\d{6})$", "USR-$1")
    )

    # Normalizar strings
    df = normalize_string(df, "country", "segment")

    # Limpiar PII
    df = df.withColumn("full_name",   F.trim(F.col("full_name")))
    df = df.withColumn("email",       F.lower(F.trim(F.col("email"))))
    df = df.withColumn("phone",       F.trim(F.col("phone")))
    df = df.withColumn("document_id", F.trim(F.col("document_id")))

    # Parsear fecha de registro
    df = parse_date_column(df, "registration_date")

    # Deduplicar
    df = deduplicate(df, ["user_id"])

    return df.select(
        "user_id", "full_name", "document_id", "email", "phone",
        "country", "segment", "registration_date",
        "_source_name", "_source_file", "_ingestion_ts", "_pipeline_run_id"
    )
