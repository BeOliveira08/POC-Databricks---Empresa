# Databricks notebook source
# run_validated_silver
#
# Bootstrap silver-only da remodelagem nova.
# Executa apenas notebooks cuja fonte obrigatoria ja exista.

# COMMAND ----------
# MAGIC %run ../config/project_params

# COMMAND ----------
# MAGIC %run ../utils/delta

# COMMAND ----------

def run_step(label: str, notebook_path: str, required_tables: list[str] | None = None):
    required_tables = required_tables or []
    missing = [tbl for tbl in required_tables if not table_exists(tbl)]
    if missing:
        print(f"[SKIP] {label} - dependencias ausentes: {', '.join(missing)}")
        return

    print(f"[START] {label}")
    dbutils.notebook.run(notebook_path, 0)
    print(f"[OK] {label}")


steps = [
    ("silver.tb_cliente_referencia", "./silver/clientes", [f"{CATALOG}.{BRONZE}.raw_clientes"]),
    ("silver.tb_ctps", "./silver/vinculo", [f"{CATALOG}.{BRONZE}.raw_ctps"]),
    ("silver.tb_inss", "./silver/inss", [f"{CATALOG}.{BRONZE}.raw_inss"]),
    ("silver.tb_fgts", "./silver/fgts", [f"{CATALOG}.{BRONZE}.raw_fgts"]),
    ("silver.tb_das", "./silver/das", [f"{CATALOG}.{BRONZE}.raw_das"]),
    ("silver.tb_notas_fiscais", "./silver/notas_fiscais", [f"{CATALOG}.{BRONZE}.raw_nf"]),
    ("silver.tb_irpf", "./silver/irpf", [f"{CATALOG}.{BRONZE}.raw_irpf"]),
    ("silver.tb_contratos", "./silver/contratos", [f"{CATALOG}.{BRONZE}.raw_contratos"]),
    ("silver.tb_propostas", "./silver/propostas", [f"{CATALOG}.{BRONZE}.raw_propostas"]),
    ("silver.tb_distratos", "./silver/distratos", [f"{CATALOG}.{BRONZE}.raw_distratos"]),
    ("silver.tb_rescisoes", "./silver/rescisoes", [f"{CATALOG}.{BRONZE}.raw_rescisoes"]),
    ("silver.tb_funcionario", "./silver/funcionarios", [f"{CATALOG}.{BRONZE}.raw_funcionarios"]),
    ("silver.tb_empresa", "./silver/empresa", []),
    (
        "silver.tb_func_emp",
        "./silver/func_emp",
        [
            f"{CATALOG}.{BRONZE}.raw_func_emp",
            f"{CATALOG}.{SILVER}.tb_funcionario",
            f"{CATALOG}.{SILVER}.tb_empresa",
        ],
    ),
    (
        "silver.tb_beneficios",
        "./silver/beneficios",
        [f"{CATALOG}.{BRONZE}.raw_beneficios", f"{CATALOG}.{SILVER}.tb_empresa"],
    ),
    ("silver.tb_reunioes_contratacao", "./silver/reunioes_contratacao", [f"{CATALOG}.{BRONZE}.raw_entrevistas"]),
]

for step in steps:
    run_step(*step)

print("[OK] Bootstrap silver finalizado com execucao condicional por dependencias")
