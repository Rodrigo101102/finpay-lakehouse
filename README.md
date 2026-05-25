# FinPay Lakehouse — Plataforma de Detección de Fraude Transaccional

## Descripción del caso de uso

FinPay es una fintech latinoamericana que procesa pagos digitales en Perú, Colombia, México, Chile y Argentina. El área de riesgo enfrentaba visibilidad limitada sobre patrones de fraude, con reportes manuales con 48 horas de retraso.

Esta plataforma construida sobre **Azure Databricks** resuelve el problema mediante:
- Pipeline automatizado de ingesta, procesamiento y publicación analítica
- Modelo dimensional consultable en tiempo casi real
- Dashboard de observabilidad sobre event logs del pipeline
- Detección de anomalías y alertas de fraude por comercio y canal

---

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────┐
│                    Azure Databricks + Unity Catalog               │
│                    Catálogo: fintech_finpay                        │
├──────────────┬──────────────┬──────────────┬─────────────────────┤
│   BRONZE     │    SILVER    │     GOLD     │   OBSERVABILITY     │
│              │              │              │                     │
│ transactions │ transactions │ txn_kpis     │ event_log           │
│ merchants    │ merchants    │ merchant_sum │                     │
│ users        │ users        │ fraud_alerts │                     │
│              │ quarantine   │              │                     │
└──────────────┴──────────────┴──────────────┴─────────────────────┘
        ▲                                           ▼
   vol_landing                              Modelo Dimensional
  (CSV/JSON/TXT)                       fact_transactions + 4 dims
```

### Capas Medallion

| Capa | Descripción |
|------|-------------|
| **Bronze** | Ingesta raw con Auto Loader (metadata-driven). Sin transformaciones. Columnas técnicas de auditoría. |
| **Silver** | Limpieza, estandarización, deduplicación, 10+ expectations, tabla de cuarentena para rechazados. |
| **Gold** | KPIs de riesgo, tasa de reversa, score de riesgo (0-100), alertas de fraude. |

### Retos técnicos implementados

- **Reto 1 — Metadata-driven**: `ingestion_archetypes.json` externaliza la configuración de cada fuente. El pipeline lee el archivo al inicio y orquesta dinámicamente sin hardcodear propiedades.
- **Reto 2 — Tabla de cuarentena**: `silver.quarantine` persiste registros rechazados con motivo, campo inválido, timestamp y contenido original para trazabilidad y reprocesamiento.

---

## Estructura del repositorio

```
finpay-lakehouse/
├── databricks.yml                          # Raíz del DAB con targets dev y prod
├── resources/
│   ├── finpay_etl_pipeline.yml             # Lakeflow Declarative Pipeline (Bronze→Silver→Gold)
│   ├── finpay_ingestion_job.yml            # Job 1: orquesta el pipeline ETL
│   ├── finpay_semantic_job.yml             # Job 2: refresca vistas materializadas
│   └── finpay_observability_dashboard.yml  # Dashboard AI/BI de observabilidad
├── src/
│   ├── utils.py                            # Funciones reutilizables compartidas
│   ├── bronze.py                           # Ingesta Bronze con @dlt.table (metadata-driven)
│   ├── silver.py                           # Transformación Silver con expectations + cuarentena
│   └── gold.py                             # Agregaciones Gold: KPIs, resumen, alertas
├── notebooks/
│   ├── 00_setup.ipynb                      # Aprovisionamiento inicial (ejecutar una vez)
│   ├── 01_create_materialized_views.sql    # Definición del modelo dimensional (una vez)
│   ├── 02_refresh_materialized_views.ipynb # REFRESH de MVs — versión referencia
│   ├── 02_refresh_materialized_views.sql   # REFRESH de MVs — ejecutado por Job 2 sobre SQL Warehouse (sql_task)
│   └── 03_observability_queries.ipynb      # Queries de validación sobre event logs
├── dashboard/
│   └── observability.lvdash.json           # Export del dashboard AI/BI
└── README.md
```

---

## Instrucciones de despliegue paso a paso

### Pre-requisitos

- Databricks CLI v0.200+ instalado: `pip install databricks-cli`
- Extensión Databricks en VS Code (opcional pero recomendada)
- Acceso admin al workspace Databricks (Azure)
- Permisos para crear catálogos en Unity Catalog

### 1. Clonar el repositorio

```bash
git clone https://github.com/<org>/finpay-lakehouse.git
cd finpay-lakehouse
```

### 2. Configurar autenticación con Databricks CLI

```bash
# Para dev
databricks configure --token --profile dev
# Ingresar: workspace URL y token personal

# Para prod
databricks configure --token --profile prod
```

### 3. Validar el bundle

```bash
databricks bundle validate
```

### 4. Aprovisionamiento inicial del workspace

Ejecutar **una sola vez** desde el workspace de Databricks:
1. Importar y ejecutar `notebooks/00_setup.ipynb`
2. Verificar en la salida que todos los checks muestran ✅
3. Subir los archivos fuente al Volume:
   - `transactions_*.csv` → `/Volumes/fintech_finpay/default/vol_landing/transactions/`
   - `merchants.json`     → `/Volumes/fintech_finpay/default/vol_landing/merchants/`
   - `users_*.txt`        → `/Volumes/fintech_finpay/default/vol_landing/users/`

### 5. Desplegar en DEV

```bash
databricks bundle deploy --target dev
```

### 6. Desplegar en PROD

```bash
databricks bundle deploy --target prod
```

### 7. Ejecutar el pipeline ETL en PROD

```bash
# Job 1: ingesta y transformación
databricks bundle run finpay_ingestion_job --target prod

# Después de que Job 1 finalice exitosamente:
# Job 2: modelo semántico
databricks bundle run finpay_semantic_job --target prod
```

### 8. Crear el modelo dimensional (una sola vez)

Desde un SQL Warehouse en el workspace, ejecutar:
```
notebooks/01_create_materialized_views.ipynb
```

### 9. Verificar el dashboard

1. En Databricks → SQL → Dashboards → buscar "FinPay Observabilidad"
2. Seleccionar rango de fechas
3. Verificar los 6 widgets con datos reales del pipeline

---

## Seguridad y permisos

| Rol | Permisos |
|-----|----------|
| `ingenieria` | USE CATALOG, CREATE TABLE, MODIFY en todos los schemas. Ve PII sin máscara. |
| `riesgo` | SELECT en `silver` y `gold`. PII enmascarado. |
| `auditoria` | SELECT en `gold` y `observability`. PII enmascarado. |

### Column Masking (silver.users)

Los campos `full_name`, `document_id`, `email` y `phone` muestran `***REDACTED***` para todos excepto el grupo `ingenieria`.

### Row-Level Security (silver.users)

Solo los grupos `ingenieria` y `riesgo` pueden leer filas de la tabla de usuarios.

---

## Variables del bundle

| Variable | Descripción | Default |
|----------|-------------|---------|
| `alert_email` | Email para alertas de fallos | `data-engineering@finpay.com` |
| `sql_warehouse_id` | ID del SQL Warehouse | *(requerido)* |
| `schedule_pause_status` | `PAUSED` o `UNPAUSED` | `PAUSED` en dev, `UNPAUSED` en prod |
| `workspace_host_dev` | URL del workspace de desarrollo | *(requerido)* |
| `workspace_host_prod` | URL del workspace de producción | *(requerido)* |
