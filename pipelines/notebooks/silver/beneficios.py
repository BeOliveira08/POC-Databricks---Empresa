# Databricks notebook source
# silver: tb_beneficios

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_BENEFICIOS = f"{CATALOG}.{BRONZE}.raw_beneficios"
BRONZE_BENEFICIOS_ITEM = f"{CATALOG}.{BRONZE}.raw_beneficios_item"
SILVER_TB_BENEFICIOS = f"{CATALOG}.{SILVER}.tb_beneficios"
SILVER_TB_BENEFICIOS_ITEM = f"{CATALOG}.{SILVER}.tb_beneficios_item"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"

money_udf = F.udf(br_money_to_float, "double")

if not table_exists(BRONZE_BENEFICIOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_BENEFICIOS}")
if not table_exists(BRONZE_BENEFICIOS_ITEM):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_BENEFICIOS_ITEM}")
if not table_exists(SILVER_TB_EMPRESA):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_EMPRESA}")


def normalized_text_expr(col_name: str):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.coalesce(repair_text(col_name), F.lit("")),
                r"\s+",
                " ",
            )
        )
    )


def name_key_expr(col_name: str):
    cleaned = F.upper(F.coalesce(repair_text(col_name), F.lit("")))
    cleaned = F.regexp_replace(cleaned, r"\b\d{6,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9]", " ")
    cleaned = F.regexp_replace(cleaned, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|S S|SLU)\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", "")
    return F.when(cleaned != "", cleaned)


def free_text_key_expr(col_name: str):
    cleaned = F.upper(F.coalesce(repair_text(col_name), F.lit("")))
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9]", "")
    return F.when(cleaned != "", cleaned)


def null_if_negative_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") |
        txt.rlike(
            r"^(NAO|NÃO|NAO INFORMADO|NÃO INFORMADO|NAO LOCALIZADO|NÃO LOCALIZADO|SEM INFORMACAO|SEM INFORMAÇÃO|N/?A|NULL|-)$"
        ),
        F.lit(None).cast("string"),
    ).otherwise(repair_text(col_name).cast("string"))


def text_flag_score(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt != "") &
        (~txt.rlike(
            r"^(NAO|NÃO|NAO INFORMADO|NÃO INFORMADO|NAO LOCALIZADO|NÃO LOCALIZADO|SEM INFORMACAO|SEM INFORMAÇÃO|N/?A|NULL|-)$"
        )),
        F.lit(1),
    ).otherwise(F.lit(0))


def benefit_status_expr(desc_col_name: str, value_col_name: str):
    raw_txt = normalized_text_expr(desc_col_name)
    return (
        F.when(F.coalesce(F.col(value_col_name), F.lit(0.0)) > 0, F.lit("VALOR_INFORMADO"))
         .when(raw_txt.rlike(r"^(NAO LOCALIZADO|NÃO LOCALIZADO)$"), F.lit("NAO LOCALIZADO"))
         .when(raw_txt == "", F.lit(None).cast("string"))
         .otherwise(F.lit("TEXTO_INFORMADO"))
    )


df_empresa = (
    spark.table(SILVER_TB_EMPRESA)
    .select(
        F.col("empresa_id").cast("bigint").alias("empresa_id_ref"),
        F.col("nu_cnpj").cast("string").alias("nu_cnpj_ref"),
        F.col("nm_empresa").cast("string").alias("nome_empresa_ref"),
    )
    .withColumn("empresa_name_key", name_key_expr("nome_empresa_ref"))
)


def enrich_company_match(df_base, row_id_col: str, name_col_name: str = "nome_empresa"):
    df_match_candidates = (
        df_base.alias("b")
        .join(
            F.broadcast(df_empresa).alias("e"),
            (
                F.col("b.nu_cnpj_empresa").isNotNull() &
                (F.col("b.nu_cnpj_empresa") == F.col("e.nu_cnpj_ref"))
            ) |
            (
                F.col("b.empresa_name_key").isNotNull() &
                (F.col("b.empresa_name_key") == F.col("e.empresa_name_key"))
            ),
            "left",
        )
        .withColumn(
            "match_tipo",
            F.when(
                F.col("b.nu_cnpj_empresa").isNotNull() &
                (F.col("b.nu_cnpj_empresa") == F.col("e.nu_cnpj_ref")),
                F.lit("CNPJ"),
            ).when(
                F.col("b.empresa_name_key").isNotNull() &
                (F.col("b.empresa_name_key") == F.col("e.empresa_name_key")),
                F.lit("NOME"),
            ),
        )
        .withColumn(
            "match_score",
            F.when(F.col("match_tipo") == "CNPJ", F.lit(3))
             .when(F.col("match_tipo") == "NOME", F.lit(2))
             .otherwise(F.lit(0))
        )
    )

    w_match = Window.partitionBy(row_id_col).orderBy(
        F.col("match_score").desc(),
        F.when(F.col("e.empresa_id_ref").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
        F.col("e.empresa_id_ref").asc_nulls_last(),
    )

    return (
        df_match_candidates
        .withColumn("rn_match", F.row_number().over(w_match))
        .filter(F.col("rn_match") == 1)
        .drop("rn_match", "match_tipo", "match_score")
        .withColumn("empresa_id", F.col("e.empresa_id_ref"))
        .withColumn(name_col_name, F.coalesce(F.col("e.nome_empresa_ref"), F.col(f"b.{name_col_name}")))
        .withColumn("nu_cnpj_empresa", F.coalesce(F.col("e.nu_cnpj_ref"), F.col("b.nu_cnpj_empresa")))
    )


df_base = (
    spark.table(BRONZE_BENEFICIOS)
    .select(
        F.col("id_empresa_raw").cast("bigint").alias("empresa_id_origem"),
        normalize_cnpj_expr(only_digits_expr("cnpj_raw")).alias("nu_cnpj_empresa"),
        normalized_text_expr("empresa_corrigida_raw").alias("empresa_corrigida"),
        normalized_text_expr("nome_operacional_raw").alias("nome_operacional"),
        normalized_text_expr("empresa_raw").alias("empresa_origem"),
        normalized_text_expr("status_mapeamento_raw").alias("status_mapeamento"),
        repair_text("observacao_mapeamento_raw").cast("string").alias("observacao_mapeamento"),
        null_if_negative_expr("plano_saude_raw").alias("plano_saude"),
        null_if_negative_expr("plano_odontologico_raw").alias("plano_odontologico"),
        null_if_negative_expr("wellhub_raw").alias("wellhub"),
        null_if_negative_expr("lazer_raw").alias("lazer"),
        null_if_negative_expr("day_off_aniversario_raw").alias("day_off_aniversario"),
        null_if_negative_expr("seguro_vida_raw").alias("seguro_vida"),
        repair_text("vr_va_raw").cast("string").alias("vr_va_raw"),
        null_if_negative_expr("nome_vr_va_raw").alias("nome_va_vr"),
        null_if_negative_expr("bem_estar_raw").alias("bem_estar"),
        null_if_negative_expr("idiomas_raw").alias("idiomas"),
        repair_text("participacao_lucro_raw").cast("string").alias("participacao_lucro"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn("beneficio_row_id", F.monotonically_increasing_id())
    .withColumn(
        "nome_empresa",
        F.coalesce(
            F.when(F.col("empresa_corrigida") != "", F.col("empresa_corrigida")),
            F.when(F.col("nome_operacional") != "", F.col("nome_operacional")),
            F.when(F.col("empresa_origem") != "", F.col("empresa_origem")),
        ),
    )
    .withColumn("empresa_name_key", name_key_expr("nome_empresa"))
    .withColumn("vr_va_valor", money_udf("vr_va_raw"))
    .withColumn("vr_va_status", benefit_status_expr("vr_va_raw", "vr_va_valor"))
    .withColumn(
        "vr_va_descricao",
        F.when(F.col("vr_va_valor").isNotNull(), F.col("nome_va_vr"))
         .otherwise(null_if_negative_expr("vr_va_raw"))
    )
    .withColumn("plr_valor", money_udf("participacao_lucro"))
    .filter(F.col("nome_empresa").isNotNull() | F.col("nu_cnpj_empresa").isNotNull() | F.col("empresa_id_origem").isNotNull())
)

df_enriched = enrich_company_match(df_base, "beneficio_row_id")

w = Window.partitionBy("dedup_key").orderBy(
    F.col("score_preenchimento").desc(),
    F.when(F.col("status_mapeamento").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_beneficio_id = Window.orderBy(
    F.coalesce(F.col("nome_empresa"), F.lit("")),
    F.coalesce(F.col("nu_cnpj_empresa"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_enriched = (
    df_enriched
    .withColumn(
        "dedup_key",
        F.coalesce(
            F.concat(F.lit("EMPRESA_ID|"), F.col("empresa_id").cast("string")),
            F.concat(F.lit("CNPJ|"), F.col("nu_cnpj_empresa")),
            F.concat(F.lit("NOME|"), F.col("b.empresa_name_key")),
        ),
    )
    .withColumn(
        "score_preenchimento",
        F.coalesce(F.when(F.col("nu_cnpj_empresa").isNotNull(), F.lit(2)), F.lit(0)) +
        F.coalesce(F.when(F.col("empresa_id").isNotNull(), F.lit(2)), F.lit(0)) +
        text_flag_score("plano_saude") +
        text_flag_score("plano_odontologico") +
        text_flag_score("wellhub") +
        text_flag_score("lazer") +
        text_flag_score("day_off_aniversario") +
        text_flag_score("seguro_vida") +
        text_flag_score("nome_va_vr") +
        text_flag_score("vr_va_descricao") +
        text_flag_score("bem_estar") +
        text_flag_score("idiomas") +
        F.when(F.coalesce(F.col("vr_va_valor"), F.lit(0.0)) > 0, F.lit(2)).otherwise(F.lit(0)) +
        F.when(F.coalesce(F.col("plr_valor"), F.lit(0.0)) > 0, F.lit(2)).otherwise(F.lit(0))
    )
)

df_final = (
    df_enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_beneficio", F.row_number().over(w_beneficio_id))
    .drop(
        "dedup_key",
        "score_preenchimento",
        "empresa_corrigida",
        "nome_operacional",
        "empresa_origem",
        "vr_va_raw",
        "participacao_lucro",
        "empresa_name_key",
        "empresa_id_ref",
        "nu_cnpj_ref",
        "nome_empresa_ref",
        "empresa_id_origem",
        "beneficio_row_id",
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_beneficio").cast("int").alias("id_beneficio"),
        F.col("empresa_id").cast("int").alias("empresa_id"),
        F.col("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("status_mapeamento").cast("string").alias("status_mapeamento"),
        F.col("observacao_mapeamento").cast("string").alias("observacao_mapeamento"),
        F.col("plano_saude").cast("string").alias("plano_saude"),
        F.col("plano_odontologico").cast("string").alias("plano_odontologico"),
        F.col("wellhub").cast("string").alias("wellhub"),
        F.col("lazer").cast("string").alias("lazer"),
        F.col("day_off_aniversario").cast("string").alias("day_off_aniversario"),
        F.col("seguro_vida").cast("string").alias("seguro_vida"),
        F.col("nome_va_vr").cast("string").alias("nome_va_vr"),
        F.col("vr_va_valor").cast("double").alias("vr_va_valor"),
        F.col("vr_va_descricao").cast("string").alias("vr_va_descricao"),
        F.col("vr_va_status").cast("string").alias("vr_va_status"),
        F.col("bem_estar").cast("string").alias("bem_estar"),
        F.col("idiomas").cast("string").alias("idiomas"),
        F.col("plr_valor").cast("double").alias("plr_valor"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(
    df_final,
    string_default="NAO INFORMADO",
    numeric_default=0,
)

write_overwrite(df_final, SILVER_TB_BENEFICIOS, catalog_schema=f"{CATALOG}.{SILVER}")


df_item_base = (
    spark.table(BRONZE_BENEFICIOS_ITEM)
    .select(
        F.col("id_empresa_raw").cast("bigint").alias("empresa_id_origem"),
        normalize_cnpj_expr(only_digits_expr("cnpj_raw")).alias("nu_cnpj_empresa"),
        normalized_text_expr("empresa_corrigida_raw").alias("empresa_corrigida"),
        normalized_text_expr("nome_operacional_raw").alias("nome_operacional"),
        normalized_text_expr("empresa_raw").alias("empresa_origem"),
        normalized_text_expr("status_mapeamento_raw").alias("status_mapeamento"),
        repair_text("observacao_mapeamento_raw").cast("string").alias("observacao_mapeamento"),
        normalized_text_expr("tipo_beneficio_raw").alias("tipo_beneficio"),
        normalized_text_expr("grupo_beneficio_raw").alias("grupo_beneficio"),
        normalized_text_expr("campo_origem_raw").alias("campo_origem"),
        null_if_negative_expr("beneficio_nome_raw").alias("nome_beneficio"),
        repair_text("beneficio_descricao_raw").cast("string").alias("beneficio_descricao_raw"),
        repair_text("beneficio_valor_raw").cast("string").alias("beneficio_valor_raw"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
    )
    .withColumn("beneficio_item_row_id", F.monotonically_increasing_id())
    .withColumn(
        "nome_empresa",
        F.coalesce(
            F.when(F.col("empresa_corrigida") != "", F.col("empresa_corrigida")),
            F.when(F.col("nome_operacional") != "", F.col("nome_operacional")),
            F.when(F.col("empresa_origem") != "", F.col("empresa_origem")),
        ),
    )
    .withColumn("empresa_name_key", name_key_expr("nome_empresa"))
    .withColumn("beneficio_descricao", null_if_negative_expr("beneficio_descricao_raw"))
    .withColumn("vl_beneficio", money_udf("beneficio_valor_raw"))
    .withColumn("status_beneficio", benefit_status_expr("beneficio_descricao_raw", "vl_beneficio"))
    .filter(
        (F.col("nome_empresa").isNotNull() | F.col("nu_cnpj_empresa").isNotNull() | F.col("empresa_id_origem").isNotNull()) &
        F.col("tipo_beneficio").isNotNull()
    )
)

df_item_enriched = enrich_company_match(df_item_base, "beneficio_item_row_id")

w_item = Window.partitionBy("dedup_item_key").orderBy(
    F.col("score_preenchimento").desc(),
    F.when(F.col("status_mapeamento").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("ingested_at_origem").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_item_id = Window.orderBy(
    F.coalesce(F.col("nome_empresa"), F.lit("")),
    F.coalesce(F.col("tipo_beneficio"), F.lit("")),
    F.coalesce(F.col("nome_beneficio"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_item_enriched = (
    df_item_enriched
    .withColumn("nome_beneficio_key", free_text_key_expr("nome_beneficio"))
    .withColumn("beneficio_desc_key", free_text_key_expr("beneficio_descricao"))
    .withColumn(
        "dedup_item_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("empresa_id").cast("string"), F.col("nu_cnpj_empresa"), F.col("b.empresa_name_key"), F.lit("")),
            F.coalesce(F.col("tipo_beneficio"), F.lit("")),
            F.coalesce(F.col("nome_beneficio_key"), F.lit("")),
            F.coalesce(F.col("beneficio_desc_key"), F.lit("")),
            F.coalesce(F.round(F.col("vl_beneficio"), 2).cast("string"), F.lit("")),
        ),
    )
    .withColumn(
        "score_preenchimento",
        F.coalesce(F.when(F.col("empresa_id").isNotNull(), F.lit(2)), F.lit(0)) +
        F.coalesce(F.when(F.col("nu_cnpj_empresa").isNotNull(), F.lit(2)), F.lit(0)) +
        text_flag_score("nome_beneficio") +
        text_flag_score("beneficio_descricao") +
        F.when(F.coalesce(F.col("vl_beneficio"), F.lit(0.0)) > 0, F.lit(2)).otherwise(F.lit(0))
    )
)

df_item_final = (
    df_item_enriched
    .withColumn("rn", F.row_number().over(w_item))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_beneficio_item", F.row_number().over(w_item_id))
    .withColumn(
        "fl_beneficio_informado",
        F.when(
            (F.coalesce(F.col("vl_beneficio"), F.lit(0.0)) > 0) |
            (text_flag_score("nome_beneficio") == 1) |
            (text_flag_score("beneficio_descricao") == 1),
            F.lit(1),
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "fl_valor_informado",
        F.when(F.coalesce(F.col("vl_beneficio"), F.lit(0.0)) > 0, F.lit(1)).otherwise(F.lit(0))
    )
    .drop(
        "dedup_item_key",
        "score_preenchimento",
        "nome_beneficio_key",
        "beneficio_desc_key",
        "empresa_corrigida",
        "nome_operacional",
        "empresa_origem",
        "empresa_name_key",
        "beneficio_item_row_id",
        "beneficio_descricao_raw",
        "beneficio_valor_raw",
        "empresa_id_origem",
        "empresa_id_ref",
        "nu_cnpj_ref",
        "nome_empresa_ref",
    )
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_beneficio_item").cast("int").alias("id_beneficio_item"),
        F.col("empresa_id").cast("int").alias("empresa_id"),
        F.col("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
        F.col("nome_empresa").cast("string").alias("nome_empresa"),
        F.col("tipo_beneficio").cast("string").alias("tipo_beneficio"),
        F.col("grupo_beneficio").cast("string").alias("grupo_beneficio"),
        F.col("campo_origem").cast("string").alias("campo_origem"),
        F.col("nome_beneficio").cast("string").alias("nome_beneficio"),
        F.col("beneficio_descricao").cast("string").alias("beneficio_descricao"),
        F.col("vl_beneficio").cast("double").alias("vl_beneficio"),
        F.col("status_beneficio").cast("string").alias("status_beneficio"),
        F.col("fl_beneficio_informado").cast("int").alias("fl_beneficio_informado"),
        F.col("fl_valor_informado").cast("int").alias("fl_valor_informado"),
        F.col("status_mapeamento").cast("string").alias("status_mapeamento"),
        F.col("observacao_mapeamento").cast("string").alias("observacao_mapeamento"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_item_final = fill_silver_nulls(
    df_item_final,
    string_default="NAO INFORMADO",
    numeric_default=0,
)

write_overwrite(df_item_final, SILVER_TB_BENEFICIOS_ITEM, catalog_schema=f"{CATALOG}.{SILVER}")

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(SILVER_TB_BENEFICIOS)
apply_governance(SILVER_TB_BENEFICIOS_ITEM)
