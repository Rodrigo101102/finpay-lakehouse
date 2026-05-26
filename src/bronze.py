# Databricks notebook source
"""
bronze.py — Ingesta Bronze 100% metadata-driven con Lakeflow Declarative Pipelines.
Reto 1: Lee ingestion_archetypes.json y crea las tablas Bronze dinámicamente.

Patrón: por cada arquetipo activo en el JSON se genera un @dlt.table automáticamente.
Para agregar una nueva fuente, solo añadir una entrada al JSON — sin modificar este código.

FinPay Lakehouse · Azure Databricks
"""

import dlt
from pyspark.sql import functions as F

from utils import (
    load_archetypes,
    add_audit_columns,
    VOLUME_PATH,
    ARCHETYPES_PATH,
)

# ---------------------------------------------------------------------------
# Cargar arquetipos al inicio del pipeline (solo fuentes activas)
# ---------------------------------------------------------------------------
_archetypes = load_archetypes(spark, ARCHETYPES_PATH)


# ---------------------------------------------------------------------------
# Helper: construir el reader de Auto Loader según el arquetipo
# ---------------------------------------------------------------------------
def _read_autoloader(archetype: dict):
    """
    Lee una fuente con Auto Loader (cloudFiles) usando las propiedades del arquetipo.

    Formatos soportados:
      - csv   → CSV estándar con delimitador y header configurables
      - text  → TXT con delimitador pipe (|); se trata internamente como CSV
      - json  → JSON multilínea o línea a línea
    """
    fmt        = archetype["file_format"].lower()
    src_path   = archetype["source_path"]
    schema_loc = archetype.get(
        "schema_location",
        f"{VOLUME_PATH}/metadata/checkpoints/{archetype['source_name']}_schema"
    )

    # Los archivos TXT con delimitador pipe se leen igual que CSV
    cloud_fmt = "csv" if fmt == "text" else fmt
    delimiter = archetype.get("delimiter") or ","
    header    = str(archetype.get("header", True)).lower()

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format",              cloud_fmt)
        .option("cloudFiles.schemaLocation",      schema_loc)
        .option("cloudFiles.inferColumnTypes",    "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    )

    if cloud_fmt == "csv":
        reader = (
            reader
            .option("sep",       delimiter)
            .option("header",    header)
            .option("multiLine", "false")
            .option("encoding",  "UTF-8")
        )

    # JSON no requiere opciones adicionales para Auto Loader

    return reader.load(src_path)


# ---------------------------------------------------------------------------
# Creación dinámica de tablas Bronze desde los arquetipos
# ---------------------------------------------------------------------------
# Por cada fuente activa en ingestion_archetypes.json se registra un @dlt.table.
# Para añadir una nueva fuente mañana: solo agregar la entrada al JSON.
# ---------------------------------------------------------------------------

def _make_bronze_table(arch: dict):
    """Fábrica de tablas Bronze: registra un @dlt.table para el arquetipo dado."""

    @dlt.table(
        name=f"bronze_{arch['source_name']}",
        comment=(
            f"Ingesta raw de '{arch['source_name']}' "
            f"({arch['file_format'].upper()}) desde {arch['source_path']}"
        ),
        table_properties={
            "delta.enableChangeDataFeed":          "true",
            "quality":                             "bronze",
            "pipelines.autoOptimize.managed":      "true",
            "source_name":                         arch["source_name"],
            "file_format":                         arch["file_format"],
        }
    )
    def _bronze_table():
        df = _read_autoloader(arch)
        return add_audit_columns(df, arch["source_name"])

    return _bronze_table


# Iterar sobre todos los arquetipos activos y registrar sus tablas
for _arch in _archetypes:
    _make_bronze_table(_arch)
