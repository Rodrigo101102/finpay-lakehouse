"""
utils.py — Funciones reutilizables compartidas por bronze, silver y gold.
FinPay Lakehouse · Azure Databricks
"""

import json
import re
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    TimestampType, DateType, DecimalType
)


# ---------------------------------------------------------------------------
# Constantes globales
# ---------------------------------------------------------------------------
CATALOG          = "fintech_finpay"
VOLUME_PATH      = f"/Volumes/{CATALOG}/default/vol_landing"
ARCHETYPES_PATH  = f"{VOLUME_PATH}/metadata/ingestion_archetypes.json"

VALID_CURRENCIES  = {"PEN", "USD", "COP", "MXN", "CLP", "ARS"}
VALID_CHANNELS    = {"web", "app", "pos"}
VALID_TX_TYPES    = {"pago", "reversa", "retiro"}
VALID_TX_STATUSES = {"aprobado", "rechazado", "pendiente"}
VALID_COUNTRIES   = {"PE", "CO", "MX", "CL", "AR"}
VALID_SEGMENTS    = {"premium", "estandar", "nuevo"}
VALID_RISK_LEVELS = {"bajo", "medio", "alto"}
VALID_MER_CATS    = {
    "retail", "restaurante", "farmacia", "supermercado",
    "tecnologia", "transporte", "educacion", "salud",
    "entretenimiento", "moda"
}
VALID_MER_STATUS  = {"activo", "inactivo", "suspendido"}


# ---------------------------------------------------------------------------
# Carga de arquetipos de ingesta
# ---------------------------------------------------------------------------
def load_archetypes(spark: SparkSession, path: str = ARCHETYPES_PATH) -> list[dict]:
    """Lee ingestion_archetypes.json y devuelve lista de dicts activos."""
    raw = spark.read.text(path).collect()
    content = "\n".join([r.value for r in raw])
    archetypes = json.loads(content)
    active = [a for a in archetypes if a.get("active", True)]
    print(f"[utils] Arquetipos activos cargados: {[a['source_name'] for a in active]}")
    return active


def get_archetype(archetypes: list[dict], source_name: str) -> Optional[dict]:
    """Devuelve el arquetipo de una fuente por nombre."""
    for a in archetypes:
        if a["source_name"] == source_name:
            return a
    raise ValueError(f"Arquetipo '{source_name}' no encontrado en archetypes.")


# ---------------------------------------------------------------------------
# Columnas técnicas de auditoría (Bronze)
# ---------------------------------------------------------------------------
def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Añade columnas técnicas estándar de auditoría para Bronze."""
    # En Unity Catalog / DLT, input_file_name() no es compatible.
    # Se debe usar la columna oculta _metadata.
    return (
        df
        .withColumn("_source_name",    F.lit(source_name))
        .withColumn("_source_file",    F.col("_metadata.file_path"))
        .withColumn("_ingestion_ts",   F.current_timestamp())
        .withColumn("_pipeline_run_id", F.expr("uuid()"))
    )


# ---------------------------------------------------------------------------
# Estandarización de fechas (Silver)
# ---------------------------------------------------------------------------
_DATE_FORMATS = ["yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "yyyyMMdd"]

def parse_date_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Intenta parsear una columna string con múltiples formatos de fecha.
    Devuelve columna de tipo DATE; NULL si ningún formato aplica.
    """
    parsed = F.lit(None).cast(DateType())
    for fmt in _DATE_FORMATS:
        parsed = F.when(
            F.to_date(F.col(col_name), fmt).isNotNull(),
            F.to_date(F.col(col_name), fmt)
        ).otherwise(parsed)
    return df.withColumn(col_name, parsed)


# ---------------------------------------------------------------------------
# Limpieza de monto (Silver)
# ---------------------------------------------------------------------------
def clean_amount(df: DataFrame, col_name: str = "amount") -> DataFrame:
    """
    Limpia la columna amount:
    - Elimina espacios, símbolos de moneda, separadores de miles
    - Normaliza coma decimal → punto decimal
    - Castea a DECIMAL(18,2)
    """
    return df.withColumn(
        col_name,
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.trim(F.col(col_name)), r"[^\d.,]", ""),
                r"\.(?=\d{3}(,|\.|$))", ""   # elimina punto como separador de miles
            ),
            ",", "."  # normaliza coma decimal
        ).cast(DecimalType(18, 2))
    )


# ---------------------------------------------------------------------------
# Validación de formatos de ID (Silver)
# ---------------------------------------------------------------------------
def validate_id_format(df: DataFrame, col_name: str, pattern: str, flag_col: str) -> DataFrame:
    """Añade columna booleana indicando si el ID cumple el patrón regex."""
    return df.withColumn(
        flag_col,
        F.col(col_name).rlike(pattern)
    )


# ---------------------------------------------------------------------------
# Estandarización de strings (lowercase + strip)
# ---------------------------------------------------------------------------
def normalize_string(df: DataFrame, *col_names: str) -> DataFrame:
    """Aplica trim + lower a las columnas indicadas."""
    for c in col_names:
        df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    return df


# ---------------------------------------------------------------------------
# Deduplicación estándar
# ---------------------------------------------------------------------------
def deduplicate(df: DataFrame, pk_cols: list[str], ts_col: str = "_ingestion_ts") -> DataFrame:
    """
    Elimina duplicados conservando el registro más reciente según ts_col.
    """
    from pyspark.sql.window import Window
    w = Window.partitionBy(*pk_cols).orderBy(F.col(ts_col).desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Tabla de cuarentena (Silver)
# ---------------------------------------------------------------------------
QUARANTINE_TABLE = f"{CATALOG}.silver.quarantine"

def write_quarantine(
    spark: SparkSession,
    df: DataFrame,
    source_name: str,
    rejection_reason: str,
    rejected_field: str = None
) -> None:
    """
    Escribe registros rechazados en silver.quarantine.
    El contenido original se serializa como JSON string.
    """
    if df.rdd.isEmpty():
        return

    quarantine_df = df.select(
        F.lit(source_name).alias("source_name"),
        F.lit(rejection_reason).alias("rejection_reason"),
        F.lit(rejected_field).alias("rejected_field"),
        F.to_json(F.struct([F.col(c) for c in df.columns])).alias("original_record"),
        F.current_timestamp().alias("processed_at"),
        F.expr("uuid()").alias("pipeline_run_id")
    )

    (
        quarantine_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )
    count = quarantine_df.count()
    print(f"[quarantine] {count} registros escritos — fuente='{source_name}' razón='{rejection_reason}'")


# ---------------------------------------------------------------------------
# Score de riesgo (Gold)
# ---------------------------------------------------------------------------
def compute_risk_score(df: DataFrame) -> DataFrame:
    """
    Calcula score_riesgo (0-100) basado en:
    - reversal_rate    (peso 0.4)
    - avg_amount       (peso 0.3, normalizado sobre 5000)
    - rejection_rate   (peso 0.3)
    Clampea entre 0 y 100.
    """
    return df.withColumn(
        "score_riesgo",
        F.least(
            F.lit(100.0),
            F.greatest(
                F.lit(0.0),
                (
                    F.col("tasa_reversa")   * F.lit(40.0) +
                    F.least(F.col("monto_promedio") / F.lit(5000.0), F.lit(1.0)) * F.lit(30.0) +
                    F.col("tasa_rechazo")   * F.lit(30.0)
                )
            )
        ).cast(DecimalType(5, 2))
    )


# ---------------------------------------------------------------------------
# Helper: imprimir estadísticas de un DataFrame
# ---------------------------------------------------------------------------
def print_stats(df: DataFrame, label: str) -> None:
    count = df.count()
    print(f"[{label}] filas={count:,}  columnas={len(df.columns)}")
