# Databricks notebook source
# run_validated_all
#
# Orquestrador geral do core atual.
# Executa bronze -> silver -> gold em sequencia.

# COMMAND ----------
# MAGIC %run ../config/project_params

# COMMAND ----------

def run_step(label: str, notebook_path: str, params: dict | None = None):
    params = params or {}
    print(f"[START] {label}")
    dbutils.notebook.run(notebook_path, 0, params)
    print(f"[OK] {label}")


base_params = {
    "catalog": CATALOG,
    "volume_path": VOL_BASE,
}

run_step("core.run_validated_core", "./run_validated_core", base_params)
run_step("silver.run_validated_silver", "./run_validated_silver", {"catalog": CATALOG})
run_step("gold.run_validated_gold", "./run_validated_gold", {"catalog": CATALOG})

print("[OK] Orquestracao geral finalizada: core -> silver -> gold")
