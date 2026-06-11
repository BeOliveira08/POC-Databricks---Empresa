# Databricks notebook source
# gold: fato_beneficios_empresa

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F

SILVER_TB_BENEFICIOS = f"{CATALOG}.{SILVER}.tb_beneficios"
GOLD_DIM_EMPRESA = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_FATO_BENEFICIOS_EMPRESA = f"{CATALOG}.{GOLD}.fato_beneficios_empresa"

if not table_exists(SILVER_TB_BENEFICIOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_BENEFICIOS}")
if not table_exists(GOLD_DIM_EMPRESA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_EMPRESA}")


def normalize_cnpj_expr(col_name: str):
    digits = F.regexp_replace(F.coalesce(F.col(col_name).cast("string"), F.lit("")), r"[^0-9]", "")
    return F.when(F.length(digits) == 14, digits).otherwise(F.lit(None).cast("string"))


def positive_text_flag(col_name: str):
    txt = F.lower(F.trim(F.coalesce(F.col(col_name).cast("string"), F.lit(""))))
    return F.when(
        (txt != "")
        & (~txt.rlike(r"^(nao|n[aã]o|sem|false|0|inexistente|n/?a|na|nao_informado)$")),
        F.lit(1),
    ).otherwise(F.lit(0))


df_empresa = (
    spark.table(GOLD_DIM_EMPRESA)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id_dim"),
        normalize_cnpj_expr("nu_cnpj").alias("nu_cnpj_dim"),
        F.col("nm_empresa").cast("string").alias("nm_empresa_dim"),
    )
)

df_beneficios = (
    spark.table(SILVER_TB_BENEFICIOS)
    .withColumn("nu_cnpj_empresa_key", normalize_cnpj_expr("nu_cnpj_empresa"))
)

df_gold = (
    df_beneficios.alias("b")
    .join(
        df_empresa.alias("e"),
        (F.col("b.empresa_id") == F.col("e.empresa_id_dim"))
        | (
            F.col("b.nu_cnpj_empresa_key").isNotNull()
            & (F.col("b.nu_cnpj_empresa_key") == F.col("e.nu_cnpj_dim"))
        ),
        "left",
    )
    .withColumn(
        "fl_tem_vr_va",
        F.when(
            (F.coalesce(F.col("b.vr_va_valor"), F.lit(0.0)) > 0)
            | (positive_text_flag("b.vr_va_descricao") == 1)
            | (positive_text_flag("b.nome_va_vr") == 1),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "fl_tem_plr",
        F.when(F.coalesce(F.col("b.plr_valor"), F.lit(0.0)) > 0, F.lit(1)).otherwise(F.lit(0)),
    )
    .withColumn("fl_plano_saude", positive_text_flag("b.plano_saude"))
    .withColumn("fl_plano_odontologico", positive_text_flag("b.plano_odontologico"))
    .withColumn("fl_wellhub", positive_text_flag("b.wellhub"))
    .withColumn("fl_lazer", positive_text_flag("b.lazer"))
    .withColumn("fl_day_off_aniversario", positive_text_flag("b.day_off_aniversario"))
    .withColumn("fl_seguro_vida", positive_text_flag("b.seguro_vida"))
    .withColumn("fl_bem_estar", positive_text_flag("b.bem_estar"))
    .withColumn("fl_idiomas", positive_text_flag("b.idiomas"))
    .select(
        F.col("b.id_beneficio").cast("bigint").alias("id_beneficio"),
        F.coalesce(F.col("e.empresa_id_dim"), F.col("b.empresa_id")).cast("bigint").alias("empresa_id"),
        F.col("b.vr_va_valor").cast("double").alias("vr_va_valor"),
        F.col("b.plr_valor").cast("double").alias("plr_valor"),
        F.col("fl_tem_vr_va").cast("int").alias("fl_tem_vr_va"),
        F.col("fl_tem_plr").cast("int").alias("fl_tem_plr"),
        F.col("fl_plano_saude").cast("int").alias("fl_plano_saude"),
        F.col("fl_plano_odontologico").cast("int").alias("fl_plano_odontologico"),
        F.col("fl_wellhub").cast("int").alias("fl_wellhub"),
        F.col("fl_lazer").cast("int").alias("fl_lazer"),
        F.col("fl_day_off_aniversario").cast("int").alias("fl_day_off_aniversario"),
        F.col("fl_seguro_vida").cast("int").alias("fl_seguro_vida"),
        F.col("fl_bem_estar").cast("int").alias("fl_bem_estar"),
        F.col("fl_idiomas").cast("int").alias("fl_idiomas"),
        F.col("b.pipeline_run_id").cast("string").alias("pipeline_run_id_origem"),
        F.col("b.ingested_at").cast("timestamp").alias("ingested_at_origem"),
        F.lit(PIPELINE_RUN_ID).cast("string").alias("pipeline_run_id"),
        F.current_timestamp().cast("timestamp").alias("ingested_at"),
    )
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_FATO_BENEFICIOS_EMPRESA} (
    id_beneficio BIGINT,
    empresa_id BIGINT,
    vr_va_valor DOUBLE,
    plr_valor DOUBLE,
    fl_tem_vr_va INT,
    fl_tem_plr INT,
    fl_plano_saude INT,
    fl_plano_odontologico INT,
    fl_wellhub INT,
    fl_lazer INT,
    fl_day_off_aniversario INT,
    fl_seguro_vida INT,
    fl_bem_estar INT,
    fl_idiomas INT,
    pipeline_run_id_origem STRING,
    ingested_at_origem TIMESTAMP,
    pipeline_run_id STRING,
    ingested_at TIMESTAMP
)
USING DELTA
""")

write_overwrite(df_gold, GOLD_FATO_BENEFICIOS_EMPRESA)

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_BENEFICIOS_EMPRESA)
