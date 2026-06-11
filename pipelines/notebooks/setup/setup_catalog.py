# Databricks notebook source
# setup_catalog — create catalog, schemas and volume directories.
# Idempotent: IF NOT EXISTS guards allow safe re-runs on any environment.

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")
print(f"Catalog '{CATALOG}' and schemas [{BRONZE}, {SILVER}, {GOLD}] are ready.")

# COMMAND ----------

for path in [
    f"{VOL_BASE}/contratos",
    f"{VOL_BASE}/propostas",
    f"{VOL_BASE}/distratos",
    f"{VOL_BASE}/rescisoes",
    f"{VOL_BASE}/ctps",
    f"{VOL_BASE}/fgts",
    f"{VOL_BASE}/inss",
    f"{VOL_BASE}/irpf",
    f"{VOL_BASE}/funcionarios",
    f"{VOL_BASE}/referencia",
    f"{VOL_BASE}/referencia/exclusividade",
    f"{VOL_BASE}/referencia/empresas",
    f"{VOL_BASE}/referencia/empresas/clt",
    f"{VOL_BASE}/referencia/empresas/pj",
    f"{VOL_BASE}/referencia/notas_emitidas",
    f"{VOL_BASE}/referencia/funcionarios",
    f"{VOL_BASE}/referencia/beneficios",
    f"{VOL_BASE}/quarantine",
    STAGING_UPLOAD_DIR,
]:
    dbutils.fs.mkdirs(path)

print(f"Volume directories ready under {VOL_BASE}")
