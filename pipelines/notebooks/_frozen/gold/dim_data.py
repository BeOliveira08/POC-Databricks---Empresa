# Databricks notebook source
# MAGIC %run ../../../config/project_params

# COMMAND ----------

# MAGIC %run ../../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

GOLD_DIM_DATA = f"{CATALOG}.{GOLD}.dim_data"

DIM_DATA_START = "2000-01-01"
DIM_DATA_END   = "2030-12-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar se a dimensÃ£o jÃ¡ existe

# COMMAND ----------

try:
    existing = spark.table(GOLD_DIM_DATA)
    row_count = existing.count()
    date_range = existing.select(
        F.min("dt_referencia").alias("min_dt"),
        F.max("dt_referencia").alias("max_dt")
    ).collect()[0]
    print(f"âœ“ dim_data existe: {row_count:,} linhas ({date_range['min_dt']} â†’ {date_range['max_dt']})")
except AnalysisException:
    print("âœ— dim_data nÃ£o existe â€” serÃ¡ criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Gerar range de datas

# COMMAND ----------

df = (
    spark.sql(f"SELECT explode(sequence(date('{DIM_DATA_START}'), date('{DIM_DATA_END}'), interval 1 day)) AS dt_referencia")

    # Surrogate keys
    .withColumn("sk_data", F.date_format("dt_referencia", "yyyyMMdd").cast("int"))
    .withColumn("sk_mes",  F.date_format("dt_referencia", "yyyyMM").cast("int"))

    # Dia
    .withColumn("nr_dia_mes",    F.dayofmonth("dt_referencia"))
    .withColumn("nr_dia_semana", F.dayofweek("dt_referencia"))
    .withColumn("ds_dia_semana", F.when(F.dayofweek("dt_referencia") == 1, "Domingo")
                                  .when(F.dayofweek("dt_referencia") == 2, "Segunda-feira")
                                  .when(F.dayofweek("dt_referencia") == 3, "TerÃ§a-feira")
                                  .when(F.dayofweek("dt_referencia") == 4, "Quarta-feira")
                                  .when(F.dayofweek("dt_referencia") == 5, "Quinta-feira")
                                  .when(F.dayofweek("dt_referencia") == 6, "Sexta-feira")
                                  .otherwise("SÃ¡bado"))
    .withColumn("ds_dia_semana_abrev", F.when(F.dayofweek("dt_referencia") == 1, "Dom")
                                        .when(F.dayofweek("dt_referencia") == 2, "Seg")
                                        .when(F.dayofweek("dt_referencia") == 3, "Ter")
                                        .when(F.dayofweek("dt_referencia") == 4, "Qua")
                                        .when(F.dayofweek("dt_referencia") == 5, "Qui")
                                        .when(F.dayofweek("dt_referencia") == 6, "Sex")
                                        .otherwise("SÃ¡b"))
    .withColumn("fl_fim_de_semana", F.when(F.dayofweek("dt_referencia").isin(1, 7), 1).otherwise(0))

    # Semana
    .withColumn("nr_semana_ano", F.weekofyear("dt_referencia"))

    # MÃªs
    .withColumn("nr_mes", F.month("dt_referencia"))
    .withColumn("ds_mes", F.when(F.month("dt_referencia") == 1,  "Janeiro")
                           .when(F.month("dt_referencia") == 2,  "Fevereiro")
                           .when(F.month("dt_referencia") == 3,  "MarÃ§o")
                           .when(F.month("dt_referencia") == 4,  "Abril")
                           .when(F.month("dt_referencia") == 5,  "Maio")
                           .when(F.month("dt_referencia") == 6,  "Junho")
                           .when(F.month("dt_referencia") == 7,  "Julho")
                           .when(F.month("dt_referencia") == 8,  "Agosto")
                           .when(F.month("dt_referencia") == 9,  "Setembro")
                           .when(F.month("dt_referencia") == 10, "Outubro")
                           .when(F.month("dt_referencia") == 11, "Novembro")
                           .otherwise("Dezembro"))
    .withColumn("ds_mes_abrev", F.when(F.month("dt_referencia") == 1,  "Jan")
                                  .when(F.month("dt_referencia") == 2,  "Fev")
                                  .when(F.month("dt_referencia") == 3,  "Mar")
                                  .when(F.month("dt_referencia") == 4,  "Abr")
                                  .when(F.month("dt_referencia") == 5,  "Mai")
                                  .when(F.month("dt_referencia") == 6,  "Jun")
                                  .when(F.month("dt_referencia") == 7,  "Jul")
                                  .when(F.month("dt_referencia") == 8,  "Ago")
                                  .when(F.month("dt_referencia") == 9,  "Set")
                                  .when(F.month("dt_referencia") == 10, "Out")
                                  .when(F.month("dt_referencia") == 11, "Nov")
                                  .otherwise("Dez"))

    # Trimestre
    .withColumn("nr_trimestre", F.quarter("dt_referencia"))
    .withColumn("ds_trimestre", F.concat(F.lit("T"), F.quarter("dt_referencia"), F.lit("-"), F.year("dt_referencia")))

    # Semestre
    .withColumn("nr_semestre", F.when(F.month("dt_referencia") <= 6, 1).otherwise(2))
    .withColumn("ds_semestre", F.concat(
        F.lit("S"),
        F.when(F.month("dt_referencia") <= 6, F.lit("1")).otherwise(F.lit("2")),
        F.lit("-"),
        F.year("dt_referencia")
    ))

    # Ano
    .withColumn("nr_ano", F.year("dt_referencia"))

    .select(
        "sk_data", "sk_mes", "dt_referencia",
        "nr_dia_mes", "nr_dia_semana", "ds_dia_semana", "ds_dia_semana_abrev", "fl_fim_de_semana",
        "nr_semana_ano",
        "nr_mes", "ds_mes", "ds_mes_abrev",
        "nr_trimestre", "ds_trimestre",
        "nr_semestre", "ds_semestre",
        "nr_ano",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar tabela e inserir

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_DIM_DATA} (
    sk_data               INT,
    sk_mes                INT,
    dt_referencia         DATE,
    nr_dia_mes            INT,
    nr_dia_semana         INT,
    ds_dia_semana         STRING,
    ds_dia_semana_abrev   STRING,
    fl_fim_de_semana      INT,
    nr_semana_ano         INT,
    nr_mes                INT,
    ds_mes                STRING,
    ds_mes_abrev          STRING,
    nr_trimestre          INT,
    ds_trimestre          STRING,
    nr_semestre           INT,
    ds_semestre           STRING,
    nr_ano                INT
)
USING DELTA
""")

write_overwrite(df, GOLD_DIM_DATA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ValidaÃ§Ã£o

# COMMAND ----------

final = spark.table(GOLD_DIM_DATA)
count = final.count()
date_range = final.select(F.min("dt_referencia"), F.max("dt_referencia")).collect()[0]

print(f"âœ“ {GOLD_DIM_DATA}: {count:,} linhas ({date_range[0]} â†’ {date_range[1]})")
final.orderBy("dt_referencia").show(5, truncate=False)

# COMMAND ----------

# MAGIC %run ../../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_DIM_DATA)

