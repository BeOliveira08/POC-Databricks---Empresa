# Databricks notebook source
# bronze: run_all_bronze

# COMMAND ----------

import time
import traceback
from datetime import datetime

# COMMAND ----------


def _current_notebook_dir():
    try:
        path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        return path.rsplit("/", 1)[0]
    except Exception:
        return "/Workspace/Shared/portfolio-lakehouse-template/pipelines/notebooks/bronze"


def _maybe_get_widget(name: str):
    try:
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else None
    except Exception:
        return None


BASE_PATH = _current_notebook_dir()
TIMEOUT = 0
STOP_ON_ERROR = True
NOTEBOOK_PARAMS = {}

catalog = _maybe_get_widget("catalog")
volume_path = _maybe_get_widget("volume_path")
if catalog:
    NOTEBOOK_PARAMS["catalog"] = catalog
if volume_path:
    NOTEBOOK_PARAMS["volume_path"] = volume_path

PIPELINE_NOTEBOOKS = [
    "empresas",
    "funcionarios_xlsx",
    "contratos_geral",
    "ctps_pdf",
    "fgts_jsonl",
    "fgts_extratos",
    "inss_jsonl",
    "inss_pdf",
    "das_pdf",
    "irpf_pdf",
    "beneficios_empresa",
    "nf_provider_a",
    "nf_portfolio",
]

pipeline_start = time.time()
results = []

print("=" * 100)
print(f"Bronze pipeline iniciado em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"BASE_PATH: {BASE_PATH}")
print(f"NOTEBOOK_PARAMS: {NOTEBOOK_PARAMS}")
print("=" * 100)

for notebook_name in PIPELINE_NOTEBOOKS:
    full_path = f"{BASE_PATH}/{notebook_name}"
    start_time = time.time()

    print(f"\n>>> Executando: {full_path}")

    try:
        output = dbutils.notebook.run(full_path, TIMEOUT, NOTEBOOK_PARAMS)
        duration = round(time.time() - start_time, 2)
        results.append({
            "notebook": notebook_name,
            "status": "SUCESSO",
            "duration_seconds": duration,
            "output": output,
        })
        print(f"    SUCESSO | {duration}s")
    except Exception:
        duration = round(time.time() - start_time, 2)
        err = traceback.format_exc()
        results.append({
            "notebook": notebook_name,
            "status": "ERRO",
            "duration_seconds": duration,
            "output": err,
        })
        print(f"    ERRO    | {duration}s")
        print(err)
        if STOP_ON_ERROR:
            print("\n[!] STOP_ON_ERROR=True - pipeline interrompido.")
            break

total_duration = round(time.time() - pipeline_start, 2)

print("\n" + "=" * 100)
print("RESUMO FINAL")
print("=" * 100)

for item in results:
    status_icon = "OK" if item["status"] == "SUCESSO" else "ERRO"
    print(f"  {status_icon:5s} | {item['notebook']:30s} | {item['duration_seconds']}s")

print(f"\nTempo total: {total_duration}s")

if any(r["status"] == "ERRO" for r in results):
    raise Exception(f"Bronze pipeline finalizado com erro(s). Tempo total: {total_duration}s")

print("\n[OK] Bronze pipeline finalizado com sucesso.")
dbutils.notebook.exit("OK")
