# Databricks notebook source
# gold: alerta_completude_documental

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"
GOLD_ALERTA_COMPLETUDE_DOCUMENTAL = f"{CATALOG}.{GOLD}.alerta_completude_documental"
SILVER_TB_BENEFICIOS = f"{CATALOG}.{SILVER}.tb_beneficios"

if not table_exists(GOLD_FATO_VINCULO_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {GOLD_FATO_VINCULO_EMPRESA}")

df_vinculo = spark.table(GOLD_FATO_VINCULO_EMPRESA)

alerta_vinculo_sem_empresa = (
    df_vinculo
    .filter(F.col("empresa_id").isNull())
    .select(
        F.lit("VINCULO").alias("dominio"),
        F.lit("fato_vinculo_empresa").alias("tabela_afetada"),
        F.lit("VINCULO").alias("granularidade_alerta"),
        F.lit("BLOQUEANTE").alias("nivel_alerta"),
        F.lit(1).cast("int").alias("fl_bloqueia_consumo"),
        F.lit("PENDENTE_VALIDACAO").alias("status_validacao"),
        F.lit("EMPRESA_ID").alias("item_pendente"),
        F.lit("Vinculo sem empresa_id valido").alias("motivo_alerta"),
        F.col("funcionario_empresa_id").cast("bigint").alias("funcionario_empresa_id"),
        F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
        F.lit(None).cast("string").alias("nu_cpf_cnpj"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.lit(None).cast("string").alias("nu_cnpj_empresa"),
        F.lit(None).cast("string").alias("nome_empresa"),
        F.lit("PF").cast("string").alias("tipo_pessoa"),
        F.lit(None).cast("string").alias("tipo_contrato"),
        F.col("dt_inicio").cast("date").alias("dt_inicio_ref"),
        F.col("dt_fim").cast("date").alias("dt_fim_ref"),
        F.lit(None).cast("int").alias("ano_ref"),
        F.col("doc_id").cast("string").alias("doc_id"),
        F.col("fonte_vinculo").cast("string").alias("fonte_vinculo"),
        F.col("nivel_confianca_vinculo").cast("string").alias("nivel_confianca_vinculo"),
        F.lit(None).cast("double").alias("vl_valor_esperado"),
        F.lit(None).cast("double").alias("vl_valor_observado"),
        F.lit(None).cast("double").alias("vl_gap_valor"),
        F.lit(None).cast("double").alias("pc_gap_valor"),
        F.concat(F.lit("VINCULO_SEM_EMPRESA|"), F.coalesce(F.col("funcionario_empresa_id").cast("string"), F.lit("SEM_ID"))).alias("chave_alerta_legivel"),
    )
)

alerta_vinculo_sem_inicio = (
    df_vinculo
    .filter(F.col("dt_inicio").isNull())
    .select(
        F.lit("VINCULO").alias("dominio"),
        F.lit("fato_vinculo_empresa").alias("tabela_afetada"),
        F.lit("VINCULO").alias("granularidade_alerta"),
        F.lit("ATENCAO").alias("nivel_alerta"),
        F.lit(0).cast("int").alias("fl_bloqueia_consumo"),
        F.lit("SEM_DATA_INICIO").alias("status_validacao"),
        F.lit("DT_INICIO").alias("item_pendente"),
        F.lit("Vinculo sem data de inicio confiavel").alias("motivo_alerta"),
        F.col("funcionario_empresa_id").cast("bigint").alias("funcionario_empresa_id"),
        F.col("id_pessoa").cast("bigint").alias("id_pessoa"),
        F.lit(None).cast("string").alias("nu_cpf_cnpj"),
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.lit(None).cast("string").alias("nu_cnpj_empresa"),
        F.lit(None).cast("string").alias("nome_empresa"),
        F.lit("PF").cast("string").alias("tipo_pessoa"),
        F.lit(None).cast("string").alias("tipo_contrato"),
        F.col("dt_inicio").cast("date").alias("dt_inicio_ref"),
        F.col("dt_fim").cast("date").alias("dt_fim_ref"),
        F.lit(None).cast("int").alias("ano_ref"),
        F.col("doc_id").cast("string").alias("doc_id"),
        F.col("fonte_vinculo").cast("string").alias("fonte_vinculo"),
        F.col("nivel_confianca_vinculo").cast("string").alias("nivel_confianca_vinculo"),
        F.lit(None).cast("double").alias("vl_valor_esperado"),
        F.lit(None).cast("double").alias("vl_valor_observado"),
        F.lit(None).cast("double").alias("vl_gap_valor"),
        F.lit(None).cast("double").alias("pc_gap_valor"),
        F.concat(F.lit("VINCULO_SEM_INICIO|"), F.coalesce(F.col("funcionario_empresa_id").cast("string"), F.lit("SEM_ID"))).alias("chave_alerta_legivel"),
    )
)

if table_exists(SILVER_TB_BENEFICIOS):
    df_beneficios = spark.table(SILVER_TB_BENEFICIOS)
    alerta_beneficio_sem_empresa = (
        df_beneficios
        .filter(F.col("empresa_id").isNull())
        .select(
            F.lit("BENEFICIOS").alias("dominio"),
            F.lit("tb_beneficios").alias("tabela_afetada"),
            F.lit("EMPRESA").alias("granularidade_alerta"),
            F.lit("ATENCAO").alias("nivel_alerta"),
            F.lit(0).cast("int").alias("fl_bloqueia_consumo"),
            F.lit("SEM_EMPRESA").alias("status_validacao"),
            F.lit("EMPRESA_ID").alias("item_pendente"),
            F.lit("Beneficio sem empresa_id resolvido").alias("motivo_alerta"),
            F.lit(None).cast("bigint").alias("funcionario_empresa_id"),
            F.lit(None).cast("bigint").alias("id_pessoa"),
            F.lit(None).cast("string").alias("nu_cpf_cnpj"),
            F.col("empresa_id").cast("bigint").alias("empresa_id"),
            F.col("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
            F.col("nome_empresa").cast("string").alias("nome_empresa"),
            F.lit(None).cast("string").alias("tipo_pessoa"),
            F.lit(None).cast("string").alias("tipo_contrato"),
            F.lit(None).cast("date").alias("dt_inicio_ref"),
            F.lit(None).cast("date").alias("dt_fim_ref"),
            F.lit(None).cast("int").alias("ano_ref"),
            F.lit(None).cast("string").alias("doc_id"),
            F.lit(None).cast("string").alias("fonte_vinculo"),
            F.lit(None).cast("string").alias("nivel_confianca_vinculo"),
            F.lit(None).cast("double").alias("vl_valor_esperado"),
            F.lit(None).cast("double").alias("vl_valor_observado"),
            F.lit(None).cast("double").alias("vl_gap_valor"),
            F.lit(None).cast("double").alias("pc_gap_valor"),
            F.concat(F.lit("BENEFICIO_SEM_EMPRESA|"), F.coalesce(F.col("id_beneficio").cast("string"), F.lit("SEM_ID"))).alias("chave_alerta_legivel"),
        )
    )
else:
    alerta_beneficio_sem_empresa = empty_df_for_schema(
        """
        dominio string, tabela_afetada string, granularidade_alerta string, nivel_alerta string,
        fl_bloqueia_consumo int, status_validacao string, item_pendente string, motivo_alerta string,
        funcionario_empresa_id bigint, id_pessoa bigint, nu_cpf_cnpj string, empresa_id bigint,
        nu_cnpj_empresa string, nome_empresa string, tipo_pessoa string,
        tipo_contrato string, dt_inicio_ref date, dt_fim_ref date, ano_ref int, doc_id string,
        fonte_vinculo string, nivel_confianca_vinculo string, vl_valor_esperado double,
        vl_valor_observado double, vl_gap_valor double, pc_gap_valor double, chave_alerta_legivel string
        """
    )

df_alertas = (
    alerta_vinculo_sem_empresa
    .unionByName(alerta_vinculo_sem_inicio)
    .unionByName(alerta_beneficio_sem_empresa)
    .withColumn("sk_alerta", F.sha2(F.coalesce(F.col("chave_alerta_legivel"), F.lit("")), 256))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("sk_alerta").cast("string").alias("sk_alerta"),
        "dominio",
        "tabela_afetada",
        "granularidade_alerta",
        "nivel_alerta",
        "fl_bloqueia_consumo",
        "status_validacao",
        "item_pendente",
        "motivo_alerta",
        "funcionario_empresa_id",
        "id_pessoa",
        "nu_cpf_cnpj",
        "empresa_id",
        "nu_cnpj_empresa",
        "nome_empresa",
        "tipo_pessoa",
        "tipo_contrato",
        "dt_inicio_ref",
        "dt_fim_ref",
        "ano_ref",
        "doc_id",
        "fonte_vinculo",
        "nivel_confianca_vinculo",
        "vl_valor_esperado",
        "vl_valor_observado",
        "vl_gap_valor",
        "pc_gap_valor",
        "chave_alerta_legivel",
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_ALERTA_COMPLETUDE_DOCUMENTAL} (
    sk_alerta STRING,
    dominio STRING,
    tabela_afetada STRING,
    granularidade_alerta STRING,
    nivel_alerta STRING,
    fl_bloqueia_consumo INT,
    status_validacao STRING,
    item_pendente STRING,
    motivo_alerta STRING,
    funcionario_empresa_id BIGINT,
    id_pessoa BIGINT,
    nu_cpf_cnpj STRING,
    empresa_id BIGINT,
    nu_cnpj_empresa STRING,
    nome_empresa STRING,
    tipo_pessoa STRING,
    tipo_contrato STRING,
    dt_inicio_ref DATE,
    dt_fim_ref DATE,
    ano_ref INT,
    doc_id STRING,
    fonte_vinculo STRING,
    nivel_confianca_vinculo STRING,
    vl_valor_esperado DOUBLE,
    vl_valor_observado DOUBLE,
    vl_gap_valor DOUBLE,
    pc_gap_valor DOUBLE,
    chave_alerta_legivel STRING,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_alertas, GOLD_ALERTA_COMPLETUDE_DOCUMENTAL)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_ALERTA_COMPLETUDE_DOCUMENTAL)
