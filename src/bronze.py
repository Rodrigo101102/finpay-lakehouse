"""
bronze.py — Ingesta Bronze metadata-driven con Lakeflow Declarative Pipelines.
Reto 1: Lee ingestion_archetypes.json y orquesta dinámicamente cada fuente.
FinPay Lakehouse · Azure Databricks
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from utils import (
    load_archetypes,
    get_archetype,
    add_audit_columns,
    CATALOG,
    VOLUME_PATH,
    ARCHETYPES_PATH,
)

# ---------------------------------------------------------------------------
# Cargar arquetipos al inicio del pipeline
# ---------------------------------------------------------------------------
_archetypes = load_archetypes(spark, ARCHETYPES_PATH)


# ---------------------------------------------------------------------------
# Helper: leer fuente con Auto Loader según arquetipo
# ---------------------------------------------------------------------------
def _read_autoloader(archetype: dict):
    """
    Construye un DataFrame de Auto Loader (cloudFiles) según el arquetipo.
    Soporta CSV, JSON y texto delimitado (TXT/pipe).
    """
    fmt        = archetype["file_format"].lower()
    src_path   = archetype["source_path"]
    schema_loc = archetype.get("schema_location", f"{VOLUME_PATH}/metadata/checkpoints/{archetype['source_name']}_schema")

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )

    if fmt == "csv":
        delimiter = archetype.get("delimiter", ",") or ","
        header    = str(archetype.get("header", True)).lower()
        reader = (
            reader
            .option("sep", delimiter)
            .option("header", header)
            .option("multiLine", "false")
            .option("encoding", "UTF-8")
        )

    elif fmt == "text":
        # Archivos .txt con delimitador pipe — se leen como líneas y se parsean
        reader = reader.option("wholeText", "false")

    # JSON no requiere opciones adicionales para Auto Loader

    return reader.load(src_path)


# ---------------------------------------------------------------------------
# Bronze: transactions
# ---------------------------------------------------------------------------
@dlt.table(
    name="transactions",
    schema=f"{CATALOG}.bronze",
    comment="Ingesta raw de transacciones CSV desde vol_landing/transactions/",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_transactions():
    archetype = get_archetype(_archetypes, "transactions")
    df = _read_autoloader(archetype)
    return add_audit_columns(df, "transactions")


# ---------------------------------------------------------------------------
# Bronze: merchants
# ---------------------------------------------------------------------------
@dlt.table(
    name="merchants",
    schema=f"{CATALOG}.bronze",
    comment="Ingesta raw de comercios JSON desde vol_landing/merchants/",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze"
    }
)
def bronze_merchants():
    archetype = get_archetype(_archetypes, "merchants")
    df = _read_autoloader(archetype)
    return add_audit_columns(df, "merchants")


# ---------------------------------------------------------------------------
# Bronze: users  (TXT con delimitador |)
# ---------------------------------------------------------------------------

# Esquema fijo para el archivo de usuarios
_USERS_SCHEMA = StructType([
    StructField("user_id",           StringType(), True),
    StructField("full_name",         StringType(), True),
    StructField("document_id",       StringType(), True),
    StructField("email",             StringType(), True),
    StructField("phone",             StringType(), True),
    StructField("country",           StringType(), True),
    StructField("segment",           StringType(), True),
    StructField("registration_date", StringType(), True),
])

_USERS_COLS = [f.name for f in _USERS_SCHEMA.fields]


@dlt.table(
    name="users",
    schema=f"{CATALOG}.bronze",
    comment="Ingesta raw de usuarios TXT (pipe-delimited) desde vol_landing/users/",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze"
    }
)
def bronze_users():
    archetype  = get_archetype(_archetypes, "users")
    src_path   = archetype["source_path"]
    delimiter  = archetype.get("delimiter", "|")
    schema_loc = archetype.get(
        "schema_location",
        f"{VOLUME_PATH}/metadata/checkpoints/users_schema"
    )

    # Leer como líneas de texto
    raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", schema_loc)
        .load(src_path)
    )

    # Parsear líneas: separar cabecera y datos en runtime
    # Filtrar línea de cabecera detectada por patrón del primer campo
    df_parsed = (
        raw
        .filter(~F.col("value").startswith("user_id"))   # excluir cabecera
        .filter(F.col("value").isNotNull())
        .filter(F.trim(F.col("value")) != "")
        .select(
            *[
                F.split(F.col("value"), r"\|").getItem(i).alias(col)
                for i, col in enumerate(_USERS_COLS)
            ]
        )
    )

    return add_audit_columns(df_parsed, "users")
