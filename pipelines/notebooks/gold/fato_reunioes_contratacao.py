# Databricks notebook source
# gold: fato_reunioes_contratacao

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_TB_REUNIOES_CONTRATACAO = f"{CATALOG}.{SILVER}.tb_reunioes_contratacao"
GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"
GOLD_DIM_EMPRESA_CONTRATACAO = f"{CATALOG}.{GOLD}.dim_empresa_contratacao"
GOLD_FATO_REUNIOES_CONTRATACAO = f"{CATALOG}.{GOLD}.fato_reunioes_contratacao"

if not table_exists(SILVER_TB_REUNIOES_CONTRATACAO):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_REUNIOES_CONTRATACAO}")
if not table_exists(GOLD_DIM_DATA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_DATA}")
if not table_exists(GOLD_DIM_EMPRESA_CONTRATACAO):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_EMPRESA_CONTRATACAO}")

df_reunioes = spark.table(SILVER_TB_REUNIOES_CONTRATACAO)
def empresa_name_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def empresa_base_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|CONSULTORIA|CONSULTING|SERVICOS|SERVICOS DE|TECNOLOGIA|INFORMATICA|DESENVOLVIMENTO|SOLUCOES|ENGENHARIA|ASSESSORIA|EMPRESARIAL|DIGITAL|SOFTWARE|SISTEMAS|DO|DA|DE|E)\b",
        " ",
    )
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


df_empresa = (
    spark.table(f"{CATALOG}.{GOLD}.dim_empresa")
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id"),
        F.col("nm_empresa").cast("string").alias("nm_empresa_dim"),
        F.col("dt_primeira_evidencia").cast("date").alias("dt_primeira_evidencia_dim"),
        F.col("dt_ultima_evidencia").cast("date").alias("dt_ultima_evidencia_dim"),
        F.col("data_inicio").cast("date").alias("data_inicio_dim"),
        F.col("data_fim").cast("date").alias("data_fim_dim"),
    )
    .withColumn("empresa_key_dim", empresa_name_key_expr(F.col("nm_empresa_dim")))
    .withColumn("empresa_base_key_dim", empresa_base_key_expr(F.col("nm_empresa_dim")))
)


df_data = spark.table(GOLD_DIM_DATA).select(
    F.col("id_data").alias("sk_data_reuniao"),
    F.col("dt_referencia").alias("dt_reuniao_ref"),
)
df_empresa_contratacao = spark.table(GOLD_DIM_EMPRESA_CONTRATACAO).select(
    F.col("id_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao"),
    F.col("empresa_contratacao_key").cast("string").alias("empresa_contratacao_key_dim"),
    F.col("empresa_id").cast("bigint").alias("empresa_id_dim_contratacao"),
)
def key_expr(col_name: str):
    return F.upper(F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^A-Z0-9]", ""))

df_base = (
    df_reunioes
    .withColumn("evento_ref_ts", F.coalesce(F.col("data_entrevista_ts"), F.col("ingested_at")))
    .withColumn("dt_reuniao", F.col("data_entrevista").cast("date"))
    .withColumn(
        "empresa_match_referencia",
        F.coalesce(
            F.col("empresa_portfolio_mapeada"),
            F.col("empresa_cliente_match"),
            F.col("empresa"),
        ),
    )
    .withColumn("empresa_key_join", empresa_name_key_expr(F.col("empresa_match_referencia")))
    .withColumn("empresa_base_key_join", empresa_base_key_expr(F.col("empresa_match_referencia")))
    .withColumn("empresa_contratacao_key_join", key_expr("empresa_contratacao"))
    .withColumn(
        "tipo_evento_join",
        F.when(F.upper(F.coalesce(F.col("assunto"), F.lit(""))).contains("ONBOARD"), F.lit("ONBOARDING"))
         .when(F.upper(F.coalesce(F.col("assunto"), F.lit(""))).contains("TREINAMENTO"), F.lit("TREINAMENTO"))
         .when(F.upper(F.coalesce(F.col("assunto"), F.lit(""))).contains("ENTREVISTA"), F.lit("ENTREVISTA"))
         .when(F.upper(F.coalesce(F.col("assunto"), F.lit(""))).contains("BATE PAPO"), F.lit("BATE_PAPO"))
         .when(F.upper(F.coalesce(F.col("assunto"), F.lit(""))).contains("REUNIAO"), F.lit("REUNIAO"))
         .otherwise(F.lit("OUTRO"))
    )
    .withColumn("plataforma_join", F.coalesce(F.col("plataforma").cast("string"), F.lit("NAO_INFORMADO")))
    .withColumn("status_reuniao_join", F.when(F.col("link_reuniao").isNotNull(), F.lit("AGENDADA")).otherwise(F.lit("NAO_INFORMADO")))
    .withColumn("empresa_match_origem_join", F.lit("EMPRESA"))
    .withColumn("confianca_match_empresa_join", F.when(F.upper(F.coalesce(F.col("status_match_empresa"), F.lit(""))).contains("MATCH"), F.lit("ALTA")).otherwise(F.lit("NAO_INFORMADO")))
    .withColumn("status_mapeamento_final_join", F.coalesce(F.col("status_match_empresa").cast("string"), F.lit("NAO_INFORMADO")))
    .withColumn(
        "processo_seletivo_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("consultoria"), F.lit("")),
                F.coalesce(F.col("empresa"), F.lit("")),
                F.coalesce(F.col("responsavel"), F.lit("")),
            ),
            256,
        ),
    )
)

w_match_empresa = Window.partitionBy("id_reuniao_contratacao").orderBy(
    F.col("match_priority").asc(),
    F.col("fl_empresa_ativa_evento").desc(),
    F.col("fl_empresa_historica_evento").desc(),
    F.col("dt_ultima_evidencia_dim").desc_nulls_last(),
    F.col("empresa_id").asc_nulls_last(),
)

df_match_empresa_exato = (
    df_base.alias("b")
    .join(
        df_empresa.alias("e"),
        F.col("b.empresa_key_join").isNotNull() & (F.col("b.empresa_key_join") == F.col("e.empresa_key_dim")),
        "left",
    )
    .withColumn("match_priority", F.lit(1))
)

df_match_empresa_base = (
    df_base.alias("b")
    .join(
        df_empresa.alias("e"),
        F.col("b.empresa_base_key_join").isNotNull() & (F.col("b.empresa_base_key_join") == F.col("e.empresa_base_key_dim")),
        "left",
    )
    .withColumn("match_priority", F.lit(2))
)

df_match_empresa = (
    df_match_empresa_exato
    .unionByName(df_match_empresa_base)
    .withColumn(
        "fl_empresa_ativa_evento",
        F.when(
            F.col("empresa_id").isNotNull()
            & F.col("dt_reuniao").isNotNull()
            & F.col("data_inicio_dim").isNotNull()
            & (F.col("dt_reuniao") >= F.col("data_inicio_dim"))
            & (
                F.col("data_fim_dim").isNull()
                | (F.col("dt_reuniao") <= F.col("data_fim_dim"))
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "fl_empresa_historica_evento",
        F.when(
            F.col("empresa_id").isNotNull()
            & F.col("dt_reuniao").isNotNull()
            & (
                (
                    F.col("dt_primeira_evidencia_dim").isNotNull()
                    & (F.col("dt_reuniao") >= F.col("dt_primeira_evidencia_dim"))
                )
                | (
                    F.col("data_inicio_dim").isNotNull()
                    & (F.col("dt_reuniao") >= F.col("data_inicio_dim"))
                )
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn("rn_match_empresa", F.row_number().over(w_match_empresa))
    .filter(F.col("rn_match_empresa") == 1)
    .drop(
        "rn_match_empresa",
        "match_priority",
        "fl_empresa_ativa_evento",
        "fl_empresa_historica_evento",
        "empresa_key_dim",
        "empresa_base_key_dim",
    )
)

df_gold = (
    df_match_empresa
    .join(
        df_empresa_contratacao,
        F.col("empresa_contratacao_key_join") == F.col("empresa_contratacao_key_dim"),
        "left",
    )
    .join(df_data, F.col("dt_reuniao").cast("date") == F.col("dt_reuniao_ref"), "left")
    .select(
        F.col("id_reuniao_contratacao").cast("bigint").alias("sk_reuniao_contratacao"),
        F.col("evento_ref_ts").cast("timestamp").alias("evento_ref_ts"),
        F.col("sk_data_reuniao").cast("int").alias("sk_data_reuniao"),
        F.col("sk_empresa_contratacao").cast("bigint").alias("sk_empresa_contratacao"),
        F.col("tipo_evento_join").cast("string").alias("tipo_evento"),
        F.col("plataforma_join").cast("string").alias("plataforma"),
        F.col("status_reuniao_join").cast("string").alias("status_reuniao"),
        F.col("empresa_match_origem_join").cast("string").alias("empresa_match_origem"),
        F.col("confianca_match_empresa_join").cast("string").alias("confianca_match_empresa"),
        F.col("status_mapeamento_final_join").cast("string").alias("status_mapeamento_final"),
        F.col("processo_seletivo_key").cast("string").alias("processo_seletivo_key"),
        F.coalesce(F.col("empresa_id"), F.col("empresa_id_dim_contratacao")).cast("bigint").alias("empresa_id"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_REUNIOES_CONTRATACAO} (
    sk_reuniao_contratacao BIGINT,
    evento_ref_ts TIMESTAMP,
    sk_data_reuniao INT,
    sk_empresa_contratacao BIGINT,
    tipo_evento STRING,
    plataforma STRING,
    status_reuniao STRING,
    empresa_match_origem STRING,
    confianca_match_empresa STRING,
    status_mapeamento_final STRING,
    processo_seletivo_key STRING,
    empresa_id BIGINT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_REUNIOES_CONTRATACAO)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_REUNIOES_CONTRATACAO)
