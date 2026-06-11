# Databricks notebook source
# run_validated_gold
#
# Bootstrap gold enxuto da remodelagem atual.
# Executa apenas os notebooks gold que fazem sentido no estado atual do projeto
# e apenas quando as dependencias minimas ja existem.

# COMMAND ----------
# MAGIC %run ../config/project_params

# COMMAND ----------
# MAGIC %run ../utils/delta

# COMMAND ----------

def run_step(
    label: str,
    notebook_path: str,
    required_all_tables: list[str] | None = None,
    required_any_tables: list[str] | None = None,
):
    required_all_tables = required_all_tables or []
    required_any_tables = required_any_tables or []

    missing_all = [tbl for tbl in required_all_tables if not table_exists(tbl)]
    if missing_all:
        print(f"[SKIP] {label} - dependencias ausentes: {', '.join(missing_all)}")
        return

    if required_any_tables and not any(table_exists(tbl) for tbl in required_any_tables):
        print(f"[SKIP] {label} - nenhuma das dependencias opcionais esta disponivel: {', '.join(required_any_tables)}")
        return

    print(f"[START] {label}")
    try:
        dbutils.notebook.run(notebook_path, 0)
    except Exception as e:
        msg = str(e).lower()
        if "notebook" in msg and ("not found" in msg or "does not exist" in msg or "resource_does_not_exist" in msg):
            print(f"[SKIP] {label} - notebook nao encontrado no workspace atual: {notebook_path}")
            return
        raise
    print(f"[OK] {label}")


steps = [
    ("gold.dim_data", "./gold/dim_data", [], []),
    (
        "gold.dim_funcionario",
        "./gold/dim_funcionario",
        [f"{CATALOG}.{SILVER}.tb_funcionario"],
        [],
    ),
    ("gold.dim_pessoa", "./gold/dim_pessoa", [f"{CATALOG}.{GOLD}.dim_funcionario"], []),
    (
        "gold.dim_cliente",
        "./gold/dim_cliente",
        [f"{CATALOG}.{SILVER}.tb_cliente_referencia"],
        [],
    ),
    ("gold.dim_empresa", "./gold/dim_empresa", [f"{CATALOG}.{SILVER}.tb_empresa"], []),
    (
        "gold.dim_cargo",
        "./gold/dim_cargo",
        [],
        [
            f"{CATALOG}.{SILVER}.tb_ctps",
            f"{CATALOG}.{SILVER}.tb_contratos",
        ],
    ),
    ("gold.dim_tipo_contrato", "./gold/dim_tipo_contrato", [], []),
    (
        "gold.dim_empresa_contratacao",
        "./gold/dim_empresa_contratacao",
        [
            f"{CATALOG}.{SILVER}.tb_reunioes_contratacao",
        ],
        [],
    ),
    (
        "gold.fato_notas_fiscais",
        "./gold/fato_notas_fiscais",
        [
            f"{CATALOG}.{SILVER}.tb_notas_fiscais",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_fgts",
        "./gold/fato_fgts",
        [
            f"{CATALOG}.{SILVER}.tb_fgts",
            f"{CATALOG}.{GOLD}.dim_data",
            f"{CATALOG}.{GOLD}.dim_pessoa",
        ],
        [],
    ),
    (
        "gold.fato_inss",
        "./gold/fato_inss",
        [
            f"{CATALOG}.{SILVER}.tb_inss",
            f"{CATALOG}.{GOLD}.dim_data",
            f"{CATALOG}.{GOLD}.dim_pessoa",
        ],
        [],
    ),
    (
        "gold.vw_resumo_previdenciario_atual",
        "./gold/vw_resumo_previdenciario_atual",
        [
            f"{CATALOG}.{GOLD}.fato_inss",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_irpf",
        "./gold/fato_irpf",
        [
            f"{CATALOG}.{SILVER}.tb_irpf",
            f"{CATALOG}.{GOLD}.dim_pessoa",
        ],
        [],
    ),
    (
        "gold.fato_imposto_serv",
        "./gold/fato_imposto_iss",
        [
            f"{CATALOG}.{SILVER}.tb_notas_fiscais",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_das",
        "./gold/fato_das",
        [
            f"{CATALOG}.{SILVER}.tb_das",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_das_semestral",
        "./gold/fato_das_semestral",
        [
            f"{CATALOG}.{GOLD}.fato_das",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_beneficios_empresa",
        "./gold/fato_beneficios_empresa",
        [
            f"{CATALOG}.{SILVER}.tb_beneficios",
            f"{CATALOG}.{GOLD}.dim_empresa",
        ],
        [],
    ),
    (
        "gold.dim_rescisao",
        "./gold/dim_rescisao",
        [
            f"{CATALOG}.{SILVER}.tb_rescisoes",
        ],
        [],
    ),
    (
        "gold.fato_rescisao_valores",
        "./gold/fato_rescisao_valores",
        [
            f"{CATALOG}.{SILVER}.tb_rescisoes",
            f"{CATALOG}.{GOLD}.dim_rescisao",
            f"{CATALOG}.{GOLD}.dim_pessoa",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_vinculo_empresa",
        "./gold/fato_vinculo_empresa",
        [
            f"{CATALOG}.{SILVER}.tb_func_emp",
            f"{CATALOG}.{SILVER}.tb_empresa",
            f"{CATALOG}.{GOLD}.dim_funcionario",
            f"{CATALOG}.{GOLD}.dim_cliente",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_funcionarios",
        "./gold/fato_funcionarios",
        [
            f"{CATALOG}.{SILVER}.tb_func_emp",
            f"{CATALOG}.{GOLD}.dim_pessoa",
        ],
        [],
    ),
    (
        "gold.fato_reunioes_contratacao",
        "./gold/fato_reunioes_contratacao",
        [
            f"{CATALOG}.{SILVER}.tb_reunioes_contratacao",
            f"{CATALOG}.{GOLD}.dim_data",
            f"{CATALOG}.{GOLD}.dim_empresa_contratacao",
        ],
        [],
    ),
    (
        "gold.fato_resultado_contratacao",
        "./gold/fato_resultado_contratacao",
        [
            f"{CATALOG}.{GOLD}.fato_reunioes_contratacao",
        ],
        [],
    ),
    (
        "gold.fato_reunioes_contratacao_mensal",
        "./gold/fato_reunioes_contratacao_mensal",
        [
            f"{CATALOG}.{GOLD}.fato_reunioes_contratacao",
        ],
        [],
    ),
    (
        "gold.bridge_vinculo_mes",
        "./gold/bridge_vinculo_mes",
        [
            f"{CATALOG}.{GOLD}.fato_vinculo_empresa",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_empresa_mensal",
        "./gold/fato_empresa_mensal",
        [
            f"{CATALOG}.{GOLD}.bridge_vinculo_mes",
            f"{CATALOG}.{GOLD}.dim_tipo_contrato",
        ],
        [],
    ),
    (
        "gold.fato_empresa_indicadores",
        "./gold/fato_empresa_indicadores",
        [
            f"{CATALOG}.{GOLD}.fato_vinculo_empresa",
            f"{CATALOG}.{GOLD}.fato_empresa_mensal",
            f"{CATALOG}.{GOLD}.dim_tipo_contrato",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
    (
        "gold.fato_ctps",
        "./gold/fato_ctps",
        [
            f"{CATALOG}.{SILVER}.tb_ctps",
            f"{CATALOG}.{GOLD}.dim_data",
        ],
        [],
    ),
]

# Temporariamente fora do runner:
# - gold.fato_empresa_contrato_resumo:
#   a logica atual ainda mistura sinais de contrato com fallback fraco de relacionamento
#   funcionario-empresa; melhor reativar quando essa semantica estiver fechada.

for step in steps:
    run_step(*step)

print("[OK] Bootstrap gold atual finalizado com execucao condicional por dependencias")
