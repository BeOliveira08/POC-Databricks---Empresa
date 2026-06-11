# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

import traceback

# COMMAND ----------

def _current_notebook_dir():
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return path.rsplit("/", 1)[0]
    except Exception:
        return "/Workspace/Shared/portfolio-lakehouse-template/gold"


BASE_PATH = _current_notebook_dir()
TIMEOUT = 0
STOP_ON_ERROR = True

PIPELINE_NOTEBOOKS = [
    "fato_fgts_dashboard",
    "fato_irpf_fonte_pagadora",
    "fato_inss_indicadores",
]

# COMMAND ----------

for notebook_name in PIPELINE_NOTEBOOKS:
    full_path = f"{BASE_PATH}/{notebook_name}"
    print(f"\n>>> Executando: {full_path}")
    try:
        dbutils.notebook.run(full_path, TIMEOUT, {})
        print("    SUCESSO")
    except Exception:
        print(traceback.format_exc())
        if STOP_ON_ERROR:
            raise

dbutils.notebook.exit("OK")

