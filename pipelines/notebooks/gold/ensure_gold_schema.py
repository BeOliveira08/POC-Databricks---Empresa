# Databricks notebook source
# gold: ensure_gold_schema

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")

required_silver_tables = [
    f"{CATALOG}.{SILVER}.tb_empresa",
    f"{CATALOG}.{SILVER}.tb_funcionario",
    f"{CATALOG}.{SILVER}.tb_func_emp",
    f"{CATALOG}.{SILVER}.tb_ctps",
    f"{CATALOG}.{SILVER}.tb_reunioes_contratacao",
    f"{CATALOG}.{SILVER}.tb_inss",
    f"{CATALOG}.{SILVER}.tb_fgts",
    f"{CATALOG}.{SILVER}.tb_das",
    f"{CATALOG}.{SILVER}.tb_notas_fiscais",
    f"{CATALOG}.{SILVER}.tb_irpf",
    f"{CATALOG}.{SILVER}.tb_beneficios",
    f"{CATALOG}.{SILVER}.tb_rescisoes",
    f"{CATALOG}.{SILVER}.tb_informe_rendimentos",
]

missing_silver_tables = [
    table_name for table_name in required_silver_tables if not table_exists(table_name)
]

if missing_silver_tables:
    missing_list = "\n - ".join(missing_silver_tables)
    print(
        "[WARN] Gold bootstrap iniciado com dependencias ausentes na silver.\n"
        f"Catalogo alvo: {CATALOG}\n"
        "O orquestrador da gold vai executar apenas os notebooks com dependencias satisfeitas.\n"
        f"Tabelas silver ainda ausentes:\n - {missing_list}"
    )
else:
    print(
        f"Catalog '{CATALOG}' and schema '{GOLD}' are ready. "
        "All required silver tables are available."
    )
