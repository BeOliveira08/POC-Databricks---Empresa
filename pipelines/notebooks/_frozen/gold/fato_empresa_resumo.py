# Databricks notebook source
# gold: fato_empresa_resumo
#
# Tabela resumo por empresa para consumo analitico/BI.
# Agrega vinculos, salarios, INSS, FGTS e beneficios em uma unica linha por empresa.

# COMMAND ----------
# MAGIC %run ../../../config/project_params

# COMMAND ----------
# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import uuid

# COMMAND ----------

# -----------------------------------------------------------------------------
# CONFIG
# -----------------------------------------------------------------------------

GOLD_DIM_EMPRESA          = f"{CATALOG}.{GOLD}.dim_empresa"
GOLD_FATO_VINCULO_EMPRESA = f"{CATALOG}.{GOLD}.fato_vinculo_empresa"
GOLD_FATO_INSS            = f"{CATALOG}.{GOLD}.fato_inss"
GOLD_FATO_FGTS            = f"{CATALOG}.{GOLD}.fato_fgts"
GOLD_FATO_EMPRESA_RESUMO  = f"{CATALOG}.{GOLD}.fato_empresa_resumo"

PIPELINE_RUN_ID = (
    str(pipeline_run_id)
    if "pipeline_run_id" in locals() and pipeline_run_id is not None
    else str(uuid.uuid4())
)

print(f"GOLD_DIM_EMPRESA          = {GOLD_DIM_EMPRESA}")
print(f"GOLD_FATO_VINCULO_EMPRESA = {GOLD_FATO_VINCULO_EMPRESA}")
print(f"GOLD_FATO_INSS            = {GOLD_FATO_INSS}")
print(f"GOLD_FATO_FGTS            = {GOLD_FATO_FGTS}")
print(f"GOLD_FATO_EMPRESA_RESUMO  = {GOLD_FATO_EMPRESA_RESUMO}")
print(f"PIPELINE_RUN_ID           = {PIPELINE_RUN_ID}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# DIMENSAO EMPRESA - OPCIONAL
# -----------------------------------------------------------------------------

if table_exists(GOLD_DIM_EMPRESA):
    df_dim = (
        spark.table(GOLD_DIM_EMPRESA)
        .withColumn(
            "nome_empresa_dim",
            F.coalesce(
                F.col("nome_legal_canonico"),
                F.col("nome_operacional"),
                F.col("nome_normalizado"),
                F.col("empresa_padrao"),
                F.col("empresa_raw")
            )
        )
        .select(
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            F.col("nu_cnpj").cast("string").alias("nu_cnpj_dim"),
            F.col("nome_empresa_dim").cast("string").alias("nome_empresa_dim"),
            F.col("grupo_empresa").cast("string").alias("grupo_empresa"),
            F.col("fl_plano_saude").cast("int").alias("fl_plano_saude"),
            F.col("fl_plano_odontologico").cast("int").alias("fl_plano_odontologico"),
            F.col("fl_wellhub").cast("int").alias("fl_wellhub"),
            F.col("fl_lazer").cast("int").alias("fl_lazer"),
            F.col("fl_day_off_aniversario").cast("int").alias("fl_day_off_aniversario"),
            F.col("fl_seguro_vida").cast("int").alias("fl_seguro_vida"),
            F.col("fl_bem_estar").cast("int").alias("fl_bem_estar"),
            F.col("fl_idiomas").cast("int").alias("fl_idiomas"),
            F.col("vr_va_valor").cast("double").alias("vr_va_valor"),
            F.col("plr_valor").cast("double").alias("plr_valor")
        )
    )
else:
    df_dim = empty_df_for_schema(
        """
        pk_empresa STRING,
        nu_cnpj_dim STRING,
        nome_empresa_dim STRING,
        grupo_empresa STRING,
        fl_plano_saude INT,
        fl_plano_odontologico INT,
        fl_wellhub INT,
        fl_lazer INT,
        fl_day_off_aniversario INT,
        fl_seguro_vida INT,
        fl_bem_estar INT,
        fl_idiomas INT,
        vr_va_valor DOUBLE,
        plr_valor DOUBLE
        """
    )

# COMMAND ----------

# -----------------------------------------------------------------------------
# VINCULOS - AGREGA METRICAS POR EMPRESA
# -----------------------------------------------------------------------------

_w_ultimo = Window.partitionBy("pk_empresa").orderBy(
    F.col("dt_inicio").desc_nulls_last(),
    F.col("dt_fim").desc_nulls_last(),
    F.col("valor_salario_mes").desc_nulls_last()
)

_w_primeiro = Window.partitionBy("pk_empresa").orderBy(
    F.col("dt_inicio").asc_nulls_last(),
    F.col("dt_fim").asc_nulls_last(),
    F.col("valor_salario_mes").asc_nulls_last()
)

df_vinc_enriq = (
    spark.table(GOLD_FATO_VINCULO_EMPRESA)
    .withColumn("_duracao_meses", F.coalesce(F.col("duracao_meses"), F.lit(0)).cast("int"))
    .withColumn("_rn_ultimo", F.row_number().over(_w_ultimo))
    .withColumn("_rn_primeiro", F.row_number().over(_w_primeiro))
    .withColumn("_sal_ultimo", F.when(F.col("_rn_ultimo") == 1, F.col("valor_salario_mes")))
    .withColumn("_sal_primeiro", F.when(F.col("_rn_primeiro") == 1, F.col("valor_salario_mes")))
    .withColumn(
        "_fl_ativo",
        F.coalesce(F.col("fl_ativo_atual"), F.lit(0)).cast("int")
    )
)

df_vinc_agg = (
    df_vinc_enriq
    .groupBy("pk_empresa")
    .agg(
        F.count("*").alias("qt_vinculos"),
        F.max("nu_cnpj").alias("nu_cnpj_vinc"),
        F.max("nome_empresa").alias("nome_empresa_vinc"),
        F.sum("_duracao_meses").alias("duracao_total_meses"),
        F.min("dt_inicio").alias("dt_inicio_primeiro_vinculo"),
        F.max(F.when(F.col("dt_fim").isNotNull(), F.col("dt_fim"))).alias("dt_fim_ultimo_vinculo"),
        F.round(F.avg("valor_salario_mes"), 2).alias("salario_medio"),
        F.max("valor_salario_mes").alias("salario_max"),
        F.max("_sal_primeiro").alias("salario_primeiro"),
        F.max("_sal_ultimo").alias("salario_ultimo"),
        F.max("_fl_ativo").alias("fl_ativo_atual"),
        F.max(
            F.when(F.upper(F.col("tp_vinculo")) == "CLT", F.lit(1)).otherwise(F.lit(0))
        ).alias("_fl_tem_clt")
    )
    .withColumn(
        "tp_vinculo_predominante",
        F.when(F.col("_fl_tem_clt") == 1, F.lit("CLT")).otherwise(F.lit("PJ"))
    )
    .drop("_fl_tem_clt")
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# INSS - OPCIONAL
# -----------------------------------------------------------------------------

if table_exists(GOLD_FATO_INSS):
    df_inss_agg = (
        spark.table(GOLD_FATO_INSS)
        .groupBy("pk_empresa")
        .agg(
            F.count("*").alias("qt_meses_inss"),
            F.round(F.sum("remuneracao_valor"), 2).alias("valor_remuneracao_total"),
            F.round(F.avg("remuneracao_valor"), 2).alias("remuneracao_media_inss")
        )
        .select(
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            F.col("qt_meses_inss").cast("bigint").alias("qt_meses_inss"),
            F.col("valor_remuneracao_total").cast("double").alias("valor_remuneracao_total"),
            F.col("remuneracao_media_inss").cast("double").alias("remuneracao_media_inss")
        )
    )
else:
    df_inss_agg = empty_df_for_schema(
        "pk_empresa STRING, qt_meses_inss BIGINT, valor_remuneracao_total DOUBLE, remuneracao_media_inss DOUBLE"
    )

# COMMAND ----------

# -----------------------------------------------------------------------------
# FGTS - OPCIONAL
# -----------------------------------------------------------------------------

if table_exists(GOLD_FATO_FGTS):
    _w_fgts_ultimo = Window.partitionBy("pk_empresa").orderBy(F.col("competencia").desc_nulls_last())

    df_fgts_enriq = (
        spark.table(GOLD_FATO_FGTS)
        .withColumn("_rn", F.row_number().over(_w_fgts_ultimo))
        .withColumn("_saldo_ultimo", F.when(F.col("_rn") == 1, F.col("saldo_fgts")))
    )

    df_fgts_agg = (
        df_fgts_enriq
        .groupBy("pk_empresa")
        .agg(
            F.count("*").alias("qt_meses_fgts"),
            F.round(F.sum("valor_fgts"), 2).alias("valor_fgts_total"),
            F.max("_saldo_ultimo").alias("saldo_fgts_ultimo")
        )
        .select(
            F.col("pk_empresa").cast("string").alias("pk_empresa"),
            F.col("qt_meses_fgts").cast("bigint").alias("qt_meses_fgts"),
            F.col("valor_fgts_total").cast("double").alias("valor_fgts_total"),
            F.col("saldo_fgts_ultimo").cast("double").alias("saldo_fgts_ultimo")
        )
    )
else:
    df_fgts_agg = empty_df_for_schema(
        "pk_empresa STRING, qt_meses_fgts BIGINT, valor_fgts_total DOUBLE, saldo_fgts_ultimo DOUBLE"
    )

# COMMAND ----------

# -----------------------------------------------------------------------------
# JOIN FINAL
# -----------------------------------------------------------------------------

df_resumo = (
    df_vinc_agg
    .join(F.broadcast(df_dim), on="pk_empresa", how="left")
    .join(F.broadcast(df_inss_agg), on="pk_empresa", how="left")
    .join(F.broadcast(df_fgts_agg), on="pk_empresa", how="left")
    .withColumn("nu_cnpj", F.coalesce(F.col("nu_cnpj_dim"), F.col("nu_cnpj_vinc")).cast("string"))
    .withColumn("nome_empresa", F.coalesce(F.col("nome_empresa_dim"), F.col("nome_empresa_vinc")).cast("string"))
    .withColumn("duracao_total_meses", F.coalesce(F.col("duracao_total_meses"), F.lit(0)).cast("int"))
    .withColumn("fl_ativo_atual", F.coalesce(F.col("fl_ativo_atual"), F.lit(0)).cast("int"))
    .withColumn("qt_meses_inss", F.coalesce(F.col("qt_meses_inss"), F.lit(0)).cast("bigint"))
    .withColumn("valor_remuneracao_total", F.coalesce(F.col("valor_remuneracao_total"), F.lit(0.0)).cast("double"))
    .withColumn("remuneracao_media_inss", F.coalesce(F.col("remuneracao_media_inss"), F.lit(0.0)).cast("double"))
    .withColumn("qt_meses_fgts", F.coalesce(F.col("qt_meses_fgts"), F.lit(0)).cast("bigint"))
    .withColumn("valor_fgts_total", F.coalesce(F.col("valor_fgts_total"), F.lit(0.0)).cast("double"))
    .withColumn("saldo_fgts_ultimo", F.coalesce(F.col("saldo_fgts_ultimo"), F.lit(0.0)).cast("double"))
    .withColumn("fl_plano_saude", F.coalesce(F.col("fl_plano_saude"), F.lit(0)).cast("int"))
    .withColumn("fl_plano_odontologico", F.coalesce(F.col("fl_plano_odontologico"), F.lit(0)).cast("int"))
    .withColumn("fl_wellhub", F.coalesce(F.col("fl_wellhub"), F.lit(0)).cast("int"))
    .withColumn("fl_lazer", F.coalesce(F.col("fl_lazer"), F.lit(0)).cast("int"))
    .withColumn("fl_day_off_aniversario", F.coalesce(F.col("fl_day_off_aniversario"), F.lit(0)).cast("int"))
    .withColumn("fl_seguro_vida", F.coalesce(F.col("fl_seguro_vida"), F.lit(0)).cast("int"))
    .withColumn("fl_bem_estar", F.coalesce(F.col("fl_bem_estar"), F.lit(0)).cast("int"))
    .withColumn("fl_idiomas", F.coalesce(F.col("fl_idiomas"), F.lit(0)).cast("int"))
    .withColumn("vr_va_valor", F.coalesce(F.col("vr_va_valor"), F.lit(0.0)).cast("double"))
    .withColumn("plr_valor", F.coalesce(F.col("plr_valor"), F.lit(0.0)).cast("double"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp().cast("timestamp"))
    .select(
        "pk_empresa",
        "nu_cnpj",
        "nome_empresa",
        "grupo_empresa",
        "qt_vinculos",
        "tp_vinculo_predominante",
        "fl_ativo_atual",
        "dt_inicio_primeiro_vinculo",
        "dt_fim_ultimo_vinculo",
        "duracao_total_meses",
        "salario_primeiro",
        "salario_ultimo",
        "salario_medio",
        "salario_max",
        "qt_meses_inss",
        "valor_remuneracao_total",
        "remuneracao_media_inss",
        "qt_meses_fgts",
        "valor_fgts_total",
        "saldo_fgts_ultimo",
        "fl_plano_saude",
        "fl_plano_odontologico",
        "fl_wellhub",
        "fl_lazer",
        "fl_day_off_aniversario",
        "fl_seguro_vida",
        "fl_bem_estar",
        "fl_idiomas",
        "vr_va_valor",
        "plr_valor",
        "pipeline_run_id",
        "ingested_at"
    )
)

# COMMAND ----------

display(
    df_resumo.select(
        "nome_empresa",
        "grupo_empresa",
        "qt_vinculos",
        "tp_vinculo_predominante",
        "fl_ativo_atual",
        "duracao_total_meses",
        "salario_ultimo",
        "remuneracao_media_inss",
        "saldo_fgts_ultimo"
    ).orderBy(F.col("fl_ativo_atual").desc(), F.col("nome_empresa"))
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
    .saveAsTable(GOLD_FATO_EMPRESA_RESUMO)
)

print(f"[OK] Tabela recriada: {GOLD_FATO_EMPRESA_RESUMO}")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS qtd_total,
            COUNT(DISTINCT pk_empresa) AS qtd_pk_empresa_distinta,
            SUM(CASE WHEN fl_ativo_atual = 1 THEN 1 ELSE 0 END) AS qtd_empresas_ativas
        FROM {GOLD_FATO_EMPRESA_RESUMO}
    """)
)

# COMMAND ----------
# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_FATO_EMPRESA_RESUMO)

