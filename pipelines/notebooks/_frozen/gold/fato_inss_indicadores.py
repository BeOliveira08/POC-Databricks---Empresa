# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------
# MAGIC %run ../../../utils/transforms

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

GOLD_FATO_INSS             = f"{CATALOG}.{GOLD}.fato_inss"
GOLD_FATO_INSS_INDICADORES = f"{CATALOG}.{GOLD}.fato_inss_indicadores"

# COMMAND ----------

if table_exists(GOLD_FATO_INSS):
    df_inss = (
        spark.table(GOLD_FATO_INSS)
        .select(
            F.col("cpf").cast("string").alias("cpf"),
            F.col("nome").cast("string").alias("nome"),
            F.col("nit").cast("string").alias("nit"),
            F.col("dt_nascimento").cast("date").alias("dt_nascimento"),
            F.col("competencia").cast("date").alias("competencia"),
            F.col("remuneracao_valor").cast("double").alias("remuneracao_valor"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id")
        )
        .filter(F.col("cpf").isNotNull())
        .filter(F.col("competencia").isNotNull())
    )
else:
    df_inss = empty_df_for_schema(
        """
        cpf STRING,
        nome STRING,
        nit STRING,
        dt_nascimento DATE,
        competencia DATE,
        remuneracao_valor DOUBLE,
        pipeline_run_id STRING
        """
    )

# COMMAND ----------

_w_12m = Window.partitionBy("cpf").orderBy(F.col("competencia").desc())

df_last_12 = (
    df_inss
    .withColumn("_rn_12m", F.row_number().over(_w_12m))
    .filter(F.col("_rn_12m") <= 12)
)

df_12m = (
    df_last_12
    .groupBy("cpf")
    .agg(
        F.round(F.avg("remuneracao_valor"), 2).alias("media_remuneracao_12m"),
        F.max("competencia").alias("ultima_competencia_12m")
    )
)

df_base = (
    df_inss
    .groupBy("cpf")
    .agg(
        F.max("nome").alias("nome"),
        F.max("nit").alias("nit"),
        F.max("dt_nascimento").alias("dt_nascimento"),
        F.min("competencia").alias("primeira_competencia"),
        F.max("competencia").alias("ultima_competencia"),
        F.countDistinct("competencia").alias("qt_meses_contribuidos"),
        F.round(F.sum("remuneracao_valor"), 2).alias("vl_remuneracao_total"),
        F.round(F.avg("remuneracao_valor"), 2).alias("media_remuneracao_total"),
        F.max("pipeline_run_id").alias("pipeline_run_id")
    )
    .join(df_12m, on="cpf", how="left")
    .withColumn("idade_atual_anos", F.floor(F.months_between(F.current_date(), F.col("dt_nascimento")) / 12))
    .withColumn("tempo_contribuicao_anos", F.round(F.col("qt_meses_contribuidos") / F.lit(12.0), 2))
    .withColumn("meses_faltantes_regra_35", F.greatest(F.lit(0), F.lit(420) - F.col("qt_meses_contribuidos")))
    .withColumn("meses_faltantes_regra_65", F.greatest(F.lit(0), F.lit(780) - F.floor(F.months_between(F.current_date(), F.col("dt_nascimento")))))
    .withColumn(
        "meses_faltantes_aposentadoria",
        F.greatest(F.col("meses_faltantes_regra_35"), F.col("meses_faltantes_regra_65"))
    )
    .withColumn("data_estimada_aposentadoria", F.add_months(F.current_date(), F.col("meses_faltantes_aposentadoria")))
    .withColumn(
        "fator_beneficio_simplificado",
        F.least(
            F.lit(1.0),
            F.lit(0.6) + F.greatest(F.lit(0.0), (F.col("qt_meses_contribuidos") - F.lit(240)) / F.lit(12.0) * F.lit(0.02))
        )
    )
    .withColumn(
        "vl_beneficio_estimado_simplificado",
        F.round(F.coalesce(F.col("media_remuneracao_12m"), F.col("media_remuneracao_total"), F.lit(0.0)) * F.col("fator_beneficio_simplificado"), 2)
    )
    .withColumn(
        "ds_metodologia",
        F.lit("Estimativa simplificada para dashboard: regra indicativa por idade 65 e contribuicao 35 anos; nao substitui calculo previdenciario oficial.")
    )
    .withColumn(
        "sk_inss_indicadores",
        F.sha2(
            F.concat_ws("||", F.coalesce(F.col("cpf"), F.lit(""))),
            256
        )
    )
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        "sk_inss_indicadores",
        "cpf",
        "nome",
        "nit",
        "dt_nascimento",
        "idade_atual_anos",
        "primeira_competencia",
        "ultima_competencia",
        "qt_meses_contribuidos",
        "tempo_contribuicao_anos",
        "meses_faltantes_regra_35",
        "meses_faltantes_regra_65",
        "meses_faltantes_aposentadoria",
        "data_estimada_aposentadoria",
        "vl_remuneracao_total",
        "media_remuneracao_total",
        "media_remuneracao_12m",
        "fator_beneficio_simplificado",
        "vl_beneficio_estimado_simplificado",
        "ds_metodologia",
        "pipeline_run_id",
        "ingested_at"
    )
)

# COMMAND ----------

write_overwrite(df_base, GOLD_FATO_INSS_INDICADORES)

