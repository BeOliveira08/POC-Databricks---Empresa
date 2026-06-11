# Databricks notebook source
# gold: fato_empresa_contrato_resumo

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_FUNCIONARIO_EMPRESA = f"{CATALOG}.{SILVER}.tb_func_emp"
SILVER_TB_EMPRESA_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_empresa_contratacao"
GOLD_FATO_EMPRESA_CONTRATO_RESUMO = f"{CATALOG}.{GOLD}.fato_empresa_contrato_resumo"

if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")
if not table_exists(SILVER_TB_EMPRESA_CONTRATACAO) and not table_exists(SILVER_TB_FUNCIONARIO_EMPRESA):
    raise ValueError(
        f"Tabela obrigatoria nao encontrada. Esperado ao menos uma de: {[SILVER_TB_EMPRESA_CONTRATACAO, SILVER_TB_FUNCIONARIO_EMPRESA]}"
    )

if table_exists(SILVER_TB_EMPRESA_CONTRATACAO):
    df_contrato = (
        spark.table(SILVER_TB_EMPRESA_CONTRATACAO)
        .groupBy("empresa_id")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("qt_contratos_observados"),
            F.concat_ws(
                " | ",
                F.sort_array(F.collect_set(F.when(F.col("tipo_contratacao").isNotNull(), F.col("tipo_contratacao")))),
            ).alias("tipos_contratacao_observados"),
            F.concat_ws(
                " | ",
                F.sort_array(F.collect_set(F.when(F.col("motivo_saida").isNotNull(), F.col("motivo_saida")))),
            ).alias("motivos_saida_observados"),
            F.min("data_inicio").alias("data_primeiro_contrato"),
            F.max("data_fim").alias("data_ultimo_contrato"),
            F.max(F.when(F.col("status_contratacao") == "ATIVA", F.lit(1)).otherwise(F.lit(0))).cast("int").alias("fl_tem_contrato_ativo"),
        )
    )
else:
    df_contrato = (
        spark.table(SILVER_TB_FUNCIONARIO_EMPRESA)
        .groupBy("empresa_id")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("qt_contratos_observados"),
            F.lit("SEM_EVIDENCIA").cast("string").alias("tipos_contratacao_observados"),
            F.lit("SEM_EVIDENCIA").cast("string").alias("motivos_saida_observados"),
            F.lit(None).cast("date").alias("data_primeiro_contrato"),
            F.lit(None).cast("date").alias("data_ultimo_contrato"),
            F.lit(0).cast("int").alias("fl_tem_contrato_ativo"),
        )
    )

df_gold = (
    spark.table(SILVER_TB_EMPRESA)
    .join(df_contrato, on="empresa_id", how="left")
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.coalesce(F.col("nu_cnpj"), F.lit("SEM_CNPJ")).cast("string").alias("nu_cnpj_empresa"),
        F.coalesce(F.col("nm_empresa"), F.lit("NAO_INFORMADO")).cast("string").alias("nome_empresa"),
        F.coalesce(F.col("tipos_contratacao_observados"), F.lit("SEM_EVIDENCIA")).cast("string").alias("clientes_observados"),
        F.lit("SEM_EVIDENCIA").cast("string").alias("cargos_observados"),
        F.lit("SEM_EVIDENCIA").cast("string").alias("senioridades_observadas"),
        F.lit(0).cast("int").alias("fl_tem_contrato_exclusivo"),
        F.lit(0).cast("int").alias("fl_tem_contrato_sem_exclusividade"),
        F.lit(0).cast("int").alias("fl_tem_clausula_nao_concorrencia"),
        F.coalesce(F.col("motivos_saida_observados"), F.lit("SEM_EVIDENCIA")).cast("string").alias("ds_exclusividade_resumo"),
        F.when(F.coalesce(F.col("qt_contratos_observados"), F.lit(0)) > 0, F.lit(1)).otherwise(F.lit(0)).cast("int").alias("fl_tem_evidencia_contrato"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_EMPRESA_CONTRATO_RESUMO} (
    empresa_id BIGINT,
    nu_cnpj_empresa STRING,
    nome_empresa STRING,
    clientes_observados STRING,
    cargos_observados STRING,
    senioridades_observadas STRING,
    fl_tem_contrato_exclusivo INT,
    fl_tem_contrato_sem_exclusividade INT,
    fl_tem_clausula_nao_concorrencia INT,
    ds_exclusividade_resumo STRING,
    fl_tem_evidencia_contrato INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_EMPRESA_CONTRATO_RESUMO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_EMPRESA_CONTRATO_RESUMO)
