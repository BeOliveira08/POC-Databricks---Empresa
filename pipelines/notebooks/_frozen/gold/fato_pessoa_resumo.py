# Databricks notebook source
# DEPRECATED: agregaÃ§Ãµes de perfil migradas para fatos especÃ­ficos.
# dim_pessoa (gold) Ã© agora o cadastro canÃ´nico dos titulares.
# Manter apenas para referÃªncia enquanto a migraÃ§Ã£o estiver em curso.

# MAGIC %run ../../../config/project_params

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

GOLD_FATO_VINCULO_MENSAL = f"{CATALOG}.{GOLD}.fato_vinculo_mensal"
GOLD_FATO_PESSOA_RESUMO  = f"{CATALOG}.{GOLD}.fato_pessoa_resumo"

PIPELINE_RUN_ID = (
    str(pipeline_run_id)
    if "pipeline_run_id" in locals() and pipeline_run_id is not None
    else str(uuid.uuid4())
)

print(f"GOLD_FATO_VINCULO_MENSAL = {GOLD_FATO_VINCULO_MENSAL}")
print(f"GOLD_FATO_PESSOA_RESUMO  = {GOLD_FATO_PESSOA_RESUMO}")
print(f"PIPELINE_RUN_ID          = {PIPELINE_RUN_ID}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# LEITURA DA FATO MENSAL CANONICA
# -----------------------------------------------------------------------------

df_fato_mensal = spark.table(GOLD_FATO_VINCULO_MENSAL)

# COMMAND ----------

# -----------------------------------------------------------------------------
# BASE POR PESSOA
# GrÃ£o final: 1 linha por pessoa
# Mantem SEM_PESSOA para registros sem resolucao de CPF/pessoa.
# -----------------------------------------------------------------------------

df_base = (
    df_fato_mensal
    .withColumn(
        "pk_pessoa_resumo",
        F.coalesce(F.col("pk_pessoa"), F.sha2(F.concat_ws("||", F.lit("SEM_PESSOA"), F.col("nu_cpf")), 256))
    )
    .withColumn(
        "id_pessoa_resumo",
        F.col("id_pessoa").cast("long")
    )
    .withColumn(
        "nu_cpf_resumo",
        F.col("nu_cpf").cast("string")
    )
    .withColumn(
        "nome_profissional_resumo",
        F.col("nome_profissional").cast("string")
    )
)

_w_ultimo = Window.partitionBy("pk_pessoa_resumo").orderBy(
    F.col("dt_competencia").desc_nulls_last(),
    F.col("dt_inicio").desc_nulls_last(),
    F.col("vl_remuneracao").desc_nulls_last()
)

_w_primeiro = Window.partitionBy("pk_pessoa_resumo").orderBy(
    F.col("dt_competencia").asc_nulls_last(),
    F.col("dt_inicio").asc_nulls_last(),
    F.col("vl_remuneracao").asc_nulls_last()
)

df_enriq = (
    df_base
    .withColumn("_rn_ultimo", F.row_number().over(_w_ultimo))
    .withColumn("_rn_primeiro", F.row_number().over(_w_primeiro))
    .withColumn("_salario_ultimo", F.when(F.col("_rn_ultimo") == 1, F.col("vl_remuneracao")))
    .withColumn("_salario_primeiro", F.when(F.col("_rn_primeiro") == 1, F.col("vl_remuneracao")))
    .withColumn("_empresa_ultima", F.when(F.col("_rn_ultimo") == 1, F.col("nome_empresa")))
    .withColumn("_cargo_ultimo", F.when(F.col("_rn_ultimo") == 1, F.col("cargo")))
    .withColumn("_tipo_ultimo", F.when(F.col("_rn_ultimo") == 1, F.col("tipo_contrato")))
)

df_resumo = (
    df_enriq
    .groupBy("pk_pessoa_resumo")
    .agg(
        F.max("id_pessoa_resumo").alias("id_pessoa"),
        F.max("nu_cpf_resumo").alias("nu_cpf"),
        F.max("nome_profissional_resumo").alias("nome_profissional"),
        F.countDistinct("pk_vinculo").alias("qt_vinculos"),
        F.countDistinct("pk_empresa").alias("qt_empresas"),
        F.countDistinct("dt_competencia").alias("qt_competencias"),
        F.min("dt_inicio").alias("dt_inicio_primeiro_vinculo"),
        F.max(F.when(F.col("dt_fim").isNotNull(), F.col("dt_fim"))).alias("dt_fim_ultimo_vinculo"),
        F.min("dt_competencia").alias("dt_competencia_primeira"),
        F.max("dt_competencia").alias("dt_competencia_ultima"),
        F.round(F.avg("vl_remuneracao"), 2).alias("remuneracao_media_mensal"),
        F.max("vl_remuneracao").alias("remuneracao_maxima_mensal"),
        F.max("_salario_primeiro").alias("remuneracao_primeira_mensal"),
        F.max("_salario_ultimo").alias("remuneracao_ultima_mensal"),
        F.max("_empresa_ultima").alias("nome_empresa_ultima"),
        F.max("_cargo_ultimo").alias("cargo_ultimo"),
        F.max("_tipo_ultimo").alias("tipo_contrato_ultimo"),
        F.max(F.when(F.col("fl_ativo_competencia") == 1, F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_vinculo_ativo"),
        F.max(F.when(F.col("fl_tem_ctps") == 1, F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_clt"),
        F.max(F.when(F.col("fl_pj") == 1, F.lit(1)).otherwise(F.lit(0))).alias("fl_tem_pj"),
        F.max("pipeline_run_id").alias("pipeline_run_id_origem"),
        F.max("ingested_at").alias("ingested_at_origem")
    )
    .withColumnRenamed("pk_pessoa_resumo", "pk_pessoa")
    .withColumn(
        "tp_vinculo_predominante",
        F.when((F.col("fl_tem_clt") == 1) & (F.col("fl_tem_pj") == 1), F.lit("CLT_E_PJ"))
         .when(F.col("fl_tem_clt") == 1, F.lit("CLT"))
         .when(F.col("fl_tem_pj") == 1, F.lit("PJ"))
         .otherwise(F.lit("NAO_IDENTIFICADO"))
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        F.col("id_pessoa").cast("long").alias("id_pessoa"),
        F.col("pk_pessoa").cast("string").alias("pk_pessoa"),
        F.col("nu_cpf").cast("string").alias("nu_cpf"),
        F.col("nome_profissional").cast("string").alias("nome_profissional"),
        F.col("qt_vinculos").cast("bigint").alias("qt_vinculos"),
        F.col("qt_empresas").cast("bigint").alias("qt_empresas"),
        F.col("qt_competencias").cast("bigint").alias("qt_competencias"),
        F.col("dt_inicio_primeiro_vinculo").cast("date").alias("dt_inicio_primeiro_vinculo"),
        F.col("dt_fim_ultimo_vinculo").cast("date").alias("dt_fim_ultimo_vinculo"),
        F.col("dt_competencia_primeira").cast("date").alias("dt_competencia_primeira"),
        F.col("dt_competencia_ultima").cast("date").alias("dt_competencia_ultima"),
        F.col("tp_vinculo_predominante").cast("string").alias("tp_vinculo_predominante"),
        F.col("fl_tem_vinculo_ativo").cast("int").alias("fl_tem_vinculo_ativo"),
        F.col("fl_tem_clt").cast("int").alias("fl_tem_clt"),
        F.col("fl_tem_pj").cast("int").alias("fl_tem_pj"),
        F.col("remuneracao_primeira_mensal").cast("double").alias("remuneracao_primeira_mensal"),
        F.col("remuneracao_ultima_mensal").cast("double").alias("remuneracao_ultima_mensal"),
        F.col("remuneracao_media_mensal").cast("double").alias("remuneracao_media_mensal"),
        F.col("remuneracao_maxima_mensal").cast("double").alias("remuneracao_maxima_mensal"),
        F.col("nome_empresa_ultima").cast("string").alias("nome_empresa_ultima"),
        F.col("cargo_ultimo").cast("string").alias("cargo_ultimo"),
        F.col("tipo_contrato_ultimo").cast("string").alias("tipo_contrato_ultimo"),
        F.col("pipeline_run_id_origem").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at")
    )
)

# COMMAND ----------

display(
    df_resumo.orderBy(F.col("fl_tem_vinculo_ativo").desc(), F.col("nome_profissional")).select(
        "id_pessoa",
        "pk_pessoa",
        "nu_cpf",
        "nome_profissional",
        "qt_vinculos",
        "qt_empresas",
        "tp_vinculo_predominante",
        "fl_tem_vinculo_ativo",
        "remuneracao_ultima_mensal",
        "nome_empresa_ultima",
        "cargo_ultimo",
        "tipo_contrato_ultimo"
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# ESCRITA FINAL
# -----------------------------------------------------------------------------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD}")

(
    df_resumo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FATO_PESSOA_RESUMO)
)

print(f"[OK] Tabela recriada: {GOLD_FATO_PESSOA_RESUMO}")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS qtd_total,
            COUNT(DISTINCT pk_pessoa) AS qtd_pk_pessoa_distinta,
            SUM(CASE WHEN fl_tem_vinculo_ativo = 1 THEN 1 ELSE 0 END) AS qtd_pessoas_com_vinculo_ativo
        FROM {GOLD_FATO_PESSOA_RESUMO}
    """)
)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_PESSOA_RESUMO)

