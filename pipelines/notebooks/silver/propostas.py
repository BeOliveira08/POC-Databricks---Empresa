# Databricks notebook source
# silver: tb_propostas

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

BRONZE_RAW_PROPOSTAS = f"{CATALOG}.{BRONZE}.raw_propostas"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_PROPOSTAS = f"{CATALOG}.{SILVER}.tb_propostas"
money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")

if not table_exists(BRONZE_RAW_PROPOSTAS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_PROPOSTAS}")


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


def cleaned_empresa_expr(col_name: str):
    cleaned = normalized_text_expr(col_name)
    cleaned = F.regexp_replace(cleaned, r"\{[^}]*\}", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA OFERTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCARTA PROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA COMERCIAL\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA DE TRABALHO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCONTRATACAO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPROPOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bOFERTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTRABALHO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bSEJA BEM VINDO A\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bSEJA BEM VINDO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTITULAR\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bJUNIOR\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bCOSTA\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bGENERICO\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bALT\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bPDF\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\bTXT\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\b\d{4,}\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)
    return F.when(
        (cleaned == "") |
        cleaned.isin("CARTA", "PROPOSTA", "OFERTA", "TRABALHO", "DE TRABALHO", "NAO INFORMADO") |
        cleaned.rlike(r"^[0-9 /.-]+$"),
        F.lit(None).cast("string"),
    ).otherwise(cleaned)


def name_key_expr(col_expr):
    return F.regexp_replace(F.coalesce(col_expr.cast("string"), F.lit("")), r"[^A-Z0-9]", "")


def base_name_key_expr(col_expr):
    cleaned = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9 ]", " ")
    cleaned = F.regexp_replace(cleaned, r"\b(SA|LTDA|EIRELI|ME|EPP|CONSULTORIA|CONSULTING|SERVICOS|SERVIÇOS|TECNOLOGIA|INFORMATICA|INFORMÁTICA|DESENVOLVIMENTO|SISTEMAS|SOLUCOES|SOLUÇÕES|RECURSOS HUMANOS|RH|DE|DO|DA|E)\b", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", "")
    return F.when(cleaned != "", cleaned)


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


empresa_refs = []

if table_exists(SILVER_TB_CTPS):
    empresa_refs.append(
        spark.table(SILVER_TB_CTPS)
        .select(
            F.col("nu_cnpj_empresa").cast("string").alias("cnpj_ref"),
            name_key_expr(normalized_text_expr("nome_empresa")).alias("empresa_key"),
            base_name_key_expr(normalized_text_expr("nome_empresa")).alias("empresa_base_key"),
        )
    )

if table_exists(SILVER_TB_CONTRATOS):
    df_contratos_ref = spark.table(SILVER_TB_CONTRATOS)
    empresa_refs.append(
        df_contratos_ref.select(
            F.col("cnpj_consultoria").cast("string").alias("cnpj_ref"),
            name_key_expr(normalized_text_expr("consultoria")).alias("empresa_key"),
            base_name_key_expr(normalized_text_expr("consultoria")).alias("empresa_base_key"),
        )
    )
    empresa_refs.append(
        df_contratos_ref
        .filter(F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) != F.lit("CLT"))
        .filter(F.length(F.regexp_replace(F.coalesce(F.col("cpf_cnpj_prestador"), F.lit("")), r"\D", "")) == 14)
        .select(
            F.col("cpf_cnpj_prestador").cast("string").alias("cnpj_ref"),
            name_key_expr(normalized_text_expr("nome_prestador")).alias("empresa_key"),
            base_name_key_expr(normalized_text_expr("nome_prestador")).alias("empresa_base_key"),
        )
    )

if table_exists(SILVER_TB_EMPRESA):
    empresa_refs.append(
        spark.table(SILVER_TB_EMPRESA)
        .select(
            F.col("nu_cnpj").cast("string").alias("cnpj_ref"),
            name_key_expr(normalized_text_expr("nm_empresa")).alias("empresa_key"),
            base_name_key_expr(normalized_text_expr("nm_empresa")).alias("empresa_base_key"),
        )
    )

if empresa_refs:
    df_empresa_ref = empresa_refs[0]
    for df_next in empresa_refs[1:]:
        df_empresa_ref = df_empresa_ref.unionByName(df_next)
    df_empresa_ref = (
        df_empresa_ref
        .filter(F.col("cnpj_ref").isNotNull() & ((F.col("empresa_key") != "") | F.col("empresa_base_key").isNotNull()))
        .dropDuplicates(["cnpj_ref", "empresa_key", "empresa_base_key"])
    )
else:
    df_empresa_ref = spark.createDataFrame([], "cnpj_ref string, empresa_key string, empresa_base_key string")

df_empresa_ref_exact = (
    df_empresa_ref
    .filter(F.col("empresa_key").isNotNull() & (F.col("empresa_key") != ""))
    .select(
        F.col("empresa_key").alias("empresa_key_ref_exact"),
        F.col("cnpj_ref").alias("cnpj_ref_exact"),
    )
    .dropDuplicates(["empresa_key_ref_exact", "cnpj_ref_exact"])
)

df_empresa_ref_base = (
    df_empresa_ref
    .filter(F.col("empresa_base_key").isNotNull())
    .select(
        F.col("empresa_base_key").alias("empresa_key_ref_base"),
        F.col("cnpj_ref").alias("cnpj_ref_base"),
    )
    .dropDuplicates(["empresa_key_ref_base", "cnpj_ref_base"])
)


df_base = (
    spark.table(BRONZE_RAW_PROPOSTAS)
    .select(
        cleaned_empresa_expr("empresa").alias("empresa"),
        normalize_cnpj_expr(only_digits_expr("cnpj")).alias("cnpj"),
        norm_text("cargo").alias("cargo"),
        norm_text("senioridade").alias("senioridade"),
        parse_date_expr("dt_inicio_prevista").alias("dt_inicio_prevista"),
        money_udf("valor_proposto").alias("vl_valor_proposto"),
        F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")).cast("string").alias("arquivo_origem"),
        F.col("arquivo_pdf_validacao").cast("string").alias("arquivo_pdf_validacao"),
    )
    .filter(F.col("empresa").isNotNull() | F.col("cnpj").isNotNull())
    .withColumn("empresa_key", name_key_expr(F.col("empresa")))
    .withColumn("empresa_base_key", base_name_key_expr(F.col("empresa")))
    .withColumn("document_key", F.coalesce(F.col("arquivo_pdf_validacao"), F.col("arquivo_origem")))
)

df_final = (
    df_base.alias("p")
    .join(
        df_empresa_ref_exact.alias("r1"),
        F.col("p.empresa_key") == F.col("r1.empresa_key_ref_exact"),
        "left",
    )
    .join(
        df_empresa_ref_base.alias("r2"),
        F.col("p.empresa_base_key") == F.col("r2.empresa_key_ref_base"),
        "left",
    )
    .withColumn("cnpj", F.coalesce(F.col("p.cnpj"), F.col("r1.cnpj_ref_exact"), F.col("r2.cnpj_ref_base")))
    .drop("empresa_key", "empresa_base_key", "empresa_key_ref_exact", "cnpj_ref_exact", "empresa_key_ref_base", "cnpj_ref_base")
    .withColumn(
        "cargo",
        F.when(
            F.trim(F.coalesce(F.col("cargo"), F.lit(""))) == "",
            F.lit("ENGENHEIRO DE DADOS"),
        ).otherwise(F.col("cargo"))
    )
    .withColumn(
        "senioridade",
        F.when(
            F.trim(F.coalesce(F.col("senioridade"), F.lit(""))) == "",
            F.lit("PLENO"),
        ).otherwise(F.col("senioridade"))
    )
    .withColumn(
        "completeness_score",
        F.when(F.col("empresa").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("cnpj").isNotNull(), F.lit(2)).otherwise(F.lit(0))
        + F.when(F.col("dt_inicio_prevista").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("vl_valor_proposto").isNotNull() & (F.col("vl_valor_proposto") > 0), F.lit(2)).otherwise(F.lit(0))
    )
)

w = Window.partitionBy(
    F.coalesce(
        F.col("document_key"),
        F.concat_ws(
            "||",
            F.coalesce(F.col("cnpj"), F.col("empresa"), F.lit("")),
            F.coalesce(F.col("cargo"), F.lit("")),
            F.coalesce(F.col("dt_inicio_prevista").cast("string"), F.lit("")),
        ),
    ),
).orderBy(
    F.col("completeness_score").desc(),
    F.col("vl_valor_proposto").desc_nulls_last(),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_proposta = Window.orderBy(
    F.coalesce(F.col("cnpj"), F.col("empresa"), F.lit("")),
    F.coalesce(F.col("cargo"), F.lit("")),
    F.coalesce(F.col("dt_inicio_prevista").cast("string"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn", "arquivo_pdf_validacao", "document_key", "completeness_score")
    .withColumn("id_proposta", F.row_number().over(w_id_proposta))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_proposta").cast("int").alias("id_proposta"),
        F.col("empresa").cast("string").alias("empresa"),
        F.col("cnpj").cast("string").alias("cnpj"),
        F.col("cargo").cast("string").alias("cargo"),
        F.col("senioridade").cast("string").alias("senioridade"),
        F.col("dt_inicio_prevista").cast("date").alias("dt_inicio_prevista"),
        F.col("vl_valor_proposto").cast("double").alias("vl_valor_proposto"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_PROPOSTAS, catalog_schema=f"{CATALOG}.{SILVER}")
