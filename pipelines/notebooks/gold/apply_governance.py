# Databricks notebook source
# apply_governance - task centralizada de governanca Gold core.
#
# Roda como ultima task do gold_pipeline core,
# aplicando metadata apenas nas tabelas ativas da esteira principal.

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

GOLD_CORE_TABLES = [
    f"{CATALOG}.{GOLD}.dim_empresa",
    f"{CATALOG}.{GOLD}.dim_data",
    f"{CATALOG}.{GOLD}.dim_pessoa",
    f"{CATALOG}.{GOLD}.dim_funcionario",
    f"{CATALOG}.{GOLD}.dim_cargo",
    f"{CATALOG}.{GOLD}.dim_tipo_contrato",
    f"{CATALOG}.{GOLD}.dim_cliente",
    f"{CATALOG}.{GOLD}.dim_empresa_contratacao",
    f"{CATALOG}.{GOLD}.dim_funcionario_operacao_atributo",
    f"{CATALOG}.{GOLD}.fato_vinculo_empresa",
    f"{CATALOG}.{GOLD}.bridge_vinculo_mes",
    f"{CATALOG}.{GOLD}.fato_empresa_mensal",
    f"{CATALOG}.{GOLD}.fato_empresa_indicadores",
    f"{CATALOG}.{GOLD}.fato_beneficios_empresa",
    f"{CATALOG}.{GOLD}.fato_das",
    f"{CATALOG}.{GOLD}.fato_das_semestral",
    f"{CATALOG}.{GOLD}.fato_notas_fiscais",
    f"{CATALOG}.{GOLD}.fato_fgts",
    f"{CATALOG}.{GOLD}.fato_inss",
    f"{CATALOG}.{GOLD}.vw_resumo_previdenciario_atual",
    f"{CATALOG}.{GOLD}.fato_irpf",
    f"{CATALOG}.{GOLD}.fato_funcionarios",
    f"{CATALOG}.{GOLD}.fato_reunioes_contratacao",
    f"{CATALOG}.{GOLD}.fato_reunioes_contratacao_mensal",
    f"{CATALOG}.{GOLD}.fato_resultado_contratacao",
    f"{CATALOG}.{GOLD}.fato_ctps",
    f"{CATALOG}.{GOLD}.fato_imposto_serv",
]

for table_name in GOLD_CORE_TABLES:
    apply_governance(table_name)
