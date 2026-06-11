# Databricks notebook source
# silver: tb_inss

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

BRONZE_INSS = f"{CATALOG}.{BRONZE}.raw_inss"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_INSS = f"{CATALOG}.{SILVER}.tb_inss"
money_udf = F.udf(br_money_to_float, "double")
date_udf = F.udf(parse_flexible_date_to_iso, "string")


def empty_inss_df():
    return empty_df_for_schema(
        """
        id_inss int,
        nu_cpf string,
        dt_nascimento date,
        seq int,
        nome_empresa string,
        nu_cnpj_empresa string,
        tp_filiado string,
        nit string,
        nr_pis_pasep string,
        dt_inicio date,
        dt_fim date,
        competencia date,
        vl_base_inss double,
        vl_contribuicao_inss double,
        arquivo_origem string,
        ingested_at_origem timestamp,
        pipeline_run_id string,
        ingested_at timestamp
        """
    )


def parse_flexible_date(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


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


def nullable_text_expr(col_name: str):
    txt = normalized_text_expr(col_name)
    return F.when(
        (txt == "") | txt.isNull() | txt.isin("NAO INFORMADO", "NÃƒO INFORMADO", "NULL", "-", "N/A"),
        F.lit(None).cast("string"),
    ).otherwise(txt)


def name_key_expr(col_expr):
    return F.regexp_replace(F.coalesce(col_expr.cast("string"), F.lit("")), r"[^A-Z0-9]", "")


def base_name_key_expr(col_expr):
    cleaned = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    cleaned = F.regexp_replace(cleaned, r"\([^)]*\)", " ")
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9 ]", " ")
    cleaned = F.regexp_replace(
        cleaned,
        (
            r"\b("
            r"SA|S A|LTDA|EIRELI|ME|EPP|HOLDING|GROUP|GRUPO|EMPRESA|EMPRESARIAL|"
            r"CONSULTORIA|CONSULTING|ASSESSORIA|SERVICOS|SERVICO|SOLUCOES|SOLUCAO|"
            r"TECNOLOGIA|TECNOLOGIAS|INFORMATICA|INFORMACAO|COMUNICACAO|SOFTWARE|"
            r"DESENVOLVIMENTO|SISTEMAS|NEGOCIOS|OUTSOURCING|RECURSOS|HUMANOS|PESSOAS|"
            r"DO|DA|DE|DAS|DOS|E|EM|BRASIL"
            r")\b"
        ),
        " ",
    )
    cleaned = F.regexp_replace(cleaned, r"\s+", "")
    return F.when(cleaned != "", cleaned)


def parenthetical_text_expr(col_name: str):
    extracted = F.regexp_extract(
        F.coalesce(repair_text(col_name), F.lit("")),
        r"\(([^)]{2,})\)",
        1,
    )
    extracted = F.upper(F.trim(F.regexp_replace(extracted, r"\s+", " ")))
    return F.when(extracted != "", extracted)


def calc_contribuicao_inss_empregado_expr(rem_col, comp_col):
    teto_2020 = 6101.06
    teto_2021 = 6433.57
    teto_2022 = 7087.22
    teto_2023 = 7507.49
    teto_2024 = 7786.02
    teto_2025 = 8157.41

    def progressive(cap1, cap2, cap3, cap4):
        return (
            F.when(rem_col <= F.lit(cap1), rem_col * F.lit(0.075))
            .when(rem_col <= F.lit(cap2), F.lit(cap1 * 0.075) + (rem_col - F.lit(cap1)) * F.lit(0.09))
            .when(rem_col <= F.lit(cap3), F.lit(cap1 * 0.075 + (cap2 - cap1) * 0.09) + (rem_col - F.lit(cap2)) * F.lit(0.12))
            .otherwise(
                F.lit(cap1 * 0.075 + (cap2 - cap1) * 0.09 + (cap3 - cap2) * 0.12)
                + (F.least(rem_col, F.lit(cap4)) - F.lit(cap3)) * F.lit(0.14)
            )
        )

    comp_year = F.year(comp_col)
    capped_legacy = F.least(rem_col, F.lit(6101.06)) * F.lit(0.11)

    return F.round(
        F.when(rem_col.isNull(), F.lit(None).cast("double"))
        .when(comp_year <= 2019, capped_legacy)
        .when(comp_year == 2020, progressive(1045.00, 2089.60, 3134.40, teto_2020))
        .when(comp_year == 2021, progressive(1100.00, 2203.48, 3305.22, teto_2021))
        .when(comp_year == 2022, progressive(1212.00, 2427.35, 3641.03, teto_2022))
        .when(comp_year == 2023, progressive(1302.00, 2571.29, 3856.94, teto_2023))
        .when(comp_year == 2024, progressive(1412.00, 2666.68, 4000.03, teto_2024))
        .otherwise(progressive(1518.00, 2793.88, 4190.83, teto_2025)),
        2,
    )


empresa_refs = []

if table_exists(SILVER_TB_CTPS):
    empresa_refs.append(
        spark.table(SILVER_TB_CTPS)
        .select(
            normalize_cnpj_expr(only_digits_expr("nu_cnpj_empresa")).alias("cnpj_ref"),
            normalized_text_expr("nome_empresa").alias("nome_ref"),
            F.col("dt_inicio").cast("date").alias("ref_dt_inicio"),
            F.col("dt_fim").cast("date").alias("ref_dt_fim"),
            F.lit(1).alias("ref_priority"),
        )
    )

if table_exists(SILVER_TB_CONTRATOS):
    empresa_refs.append(
        spark.table(SILVER_TB_CONTRATOS)
        .filter(F.upper(F.coalesce(F.col("tp_vinculo"), F.lit(""))) == F.lit("CLT"))
        .select(
            normalize_cnpj_expr(only_digits_expr("cnpj_consultoria")).alias("cnpj_ref"),
            normalized_text_expr("consultoria").alias("nome_ref"),
            F.col("dt_inicio").cast("date").alias("ref_dt_inicio"),
            F.col("dt_fim").cast("date").alias("ref_dt_fim"),
            F.lit(2).alias("ref_priority"),
        )
    )

if table_exists(SILVER_TB_EMPRESA):
    empresa_refs.append(
        spark.table(SILVER_TB_EMPRESA)
        .select(
            normalize_cnpj_expr(only_digits_expr("nu_cnpj")).alias("cnpj_ref"),
            normalized_text_expr("nm_empresa").alias("nome_ref"),
            F.lit(None).cast("date").alias("ref_dt_inicio"),
            F.lit(None).cast("date").alias("ref_dt_fim"),
            F.lit(3).alias("ref_priority"),
        )
    )

if empresa_refs:
    df_empresa_ref = empresa_refs[0]
    for df_next in empresa_refs[1:]:
        df_empresa_ref = df_empresa_ref.unionByName(df_next)

    df_empresa_ref_alias = (
        df_empresa_ref
        .select(
            "cnpj_ref",
            parenthetical_text_expr("nome_ref").alias("nome_ref"),
            "ref_dt_inicio",
            "ref_dt_fim",
            "ref_priority",
        )
        .filter(F.col("nome_ref").isNotNull())
    )

    df_empresa_ref = (
        df_empresa_ref.unionByName(df_empresa_ref_alias)
        .filter(F.col("cnpj_ref").isNotNull() & F.col("nome_ref").isNotNull())
        .withColumn("empresa_key", name_key_expr(F.col("nome_ref")))
        .withColumn("empresa_base_key", base_name_key_expr(F.col("nome_ref")))
        .filter((F.col("empresa_key") != "") | F.col("empresa_base_key").isNotNull())
        .dropDuplicates(["cnpj_ref", "nome_ref", "ref_dt_inicio", "ref_dt_fim", "ref_priority"])
    )
else:
    df_empresa_ref = spark.createDataFrame(
        [],
        "cnpj_ref string, nome_ref string, ref_dt_inicio date, ref_dt_fim date, ref_priority int, empresa_key string, empresa_base_key string",
    )


if not table_exists(BRONZE_INSS):
    df_final = empty_inss_df()
else:
    df_base = (
        spark.table(BRONZE_INSS)
        .select(
            normalize_cpf_expr(only_digits_expr("cpf")).alias("nu_cpf"),
            parse_flexible_date("dt_nascimento").alias("dt_nascimento"),
            F.col("seq").cast("int").alias("seq"),
            nullable_text_expr("empresa").alias("nome_empresa"),
            normalize_cnpj_expr(only_digits_expr("cnpj")).alias("nu_cnpj_empresa"),
            nullable_text_expr("tipo_filiado").alias("tp_filiado"),
            only_digits_expr("nit").alias("nit"),
            only_digits_expr("nit").alias("nr_pis_pasep"),
            parse_flexible_date("dt_inicio").alias("dt_inicio"),
            parse_flexible_date("dt_fim").alias("dt_fim"),
            parse_flexible_date("competencia").alias("competencia"),
            money_udf("vl_base_inss").alias("vl_base_inss"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at_origem"),
        )
        .filter(F.col("competencia").isNotNull())
        .filter(F.col("nome_empresa").isNotNull() | F.col("nu_cnpj_empresa").isNotNull())
        .withColumn("row_uid", F.monotonically_increasing_id())
        .withColumn("empresa_key", name_key_expr(normalized_text_expr("nome_empresa")))
        .withColumn("empresa_base_key", base_name_key_expr(normalized_text_expr("nome_empresa")))
    )

    if empresa_refs:
        df_ref_exact = (
            df_empresa_ref
            .filter(F.col("empresa_key").isNotNull() & (F.col("empresa_key") != ""))
            .select("cnpj_ref", "empresa_key", "ref_dt_inicio", "ref_dt_fim", "ref_priority")
        )

        df_ref_base = (
            df_empresa_ref
            .filter(F.col("empresa_base_key").isNotNull())
            .select("cnpj_ref", "empresa_base_key", "ref_dt_inicio", "ref_dt_fim", "ref_priority")
        )

        df_candidates_exact = (
            df_base.alias("b")
            .join(
                df_ref_exact.alias("r"),
                F.col("b.empresa_key") == F.col("r.empresa_key"),
                "left",
            )
            .select(
                F.col("b.row_uid").alias("row_uid"),
                F.col("r.cnpj_ref").alias("cnpj_ref"),
                F.col("r.ref_dt_inicio").alias("ref_dt_inicio"),
                F.col("r.ref_dt_fim").alias("ref_dt_fim"),
                F.col("r.ref_priority").alias("ref_priority"),
                F.lit(2).alias("match_level"),
            )
        )

        df_candidates_base = (
            df_base.alias("b")
            .join(
                df_ref_base.alias("r"),
                F.col("b.empresa_base_key") == F.col("r.empresa_base_key"),
                "left",
            )
            .select(
                F.col("b.row_uid").alias("row_uid"),
                F.col("r.cnpj_ref").alias("cnpj_ref"),
                F.col("r.ref_dt_inicio").alias("ref_dt_inicio"),
                F.col("r.ref_dt_fim").alias("ref_dt_fim"),
                F.col("r.ref_priority").alias("ref_priority"),
                F.lit(1).alias("match_level"),
            )
        )

        w_ref = Window.partitionBy("row_uid").orderBy(
            F.col("match_level").desc(),
            F.col("date_match_score").desc(),
            F.col("ref_priority").asc_nulls_last(),
            F.col("has_ref_dates").desc(),
            F.col("cnpj_ref").asc_nulls_last(),
        )

        df_best_ref = (
            df_candidates_exact.unionByName(df_candidates_base)
            .join(
                df_base.select("row_uid", "competencia", "dt_inicio", "dt_fim"),
                on="row_uid",
                how="inner",
            )
            .filter(F.col("cnpj_ref").isNotNull())
            .withColumn(
                "has_ref_dates",
                F.when(F.col("ref_dt_inicio").isNotNull() | F.col("ref_dt_fim").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
            )
            .withColumn(
                "match_point",
                F.coalesce(F.col("competencia"), F.col("dt_inicio"), F.col("dt_fim")),
            )
            .withColumn(
                "date_match_score",
                F.when(
                    F.col("has_ref_dates") == 1,
                    F.when(
                        F.col("match_point").between(
                            F.coalesce(F.col("ref_dt_inicio"), F.to_date(F.lit("1900-01-01"))),
                            F.coalesce(F.col("ref_dt_fim"), F.to_date(F.lit("2999-12-31"))),
                        ),
                        F.lit(1),
                    ).otherwise(F.lit(0)),
                ).otherwise(F.lit(0)),
            )
            .withColumn("rn", F.row_number().over(w_ref))
            .filter(F.col("rn") == 1)
            .select(
                F.col("row_uid"),
                F.col("cnpj_ref").alias("cnpj_ref_best"),
            )
        )

        df_base = (
            df_base
            .join(df_best_ref, on="row_uid", how="left")
            .withColumn("nu_cnpj_empresa", F.coalesce(F.col("nu_cnpj_empresa"), F.col("cnpj_ref_best")))
            .drop("cnpj_ref_best")
        )

    df_final = (
        df_base
        .withColumn(
            "vl_contribuicao_inss",
            F.when(
                F.upper(F.coalesce(F.col("tp_filiado"), F.lit(""))).contains("EMPREGADO"),
                calc_contribuicao_inss_empregado_expr(F.col("vl_base_inss"), F.col("competencia")),
            ),
        )
        .drop("row_uid", "empresa_key", "empresa_base_key")
    )

if table_exists(BRONZE_INSS):
    w = Window.partitionBy(
        F.coalesce(F.col("nu_cpf"), F.lit("")),
        F.coalesce(F.col("nu_cnpj_empresa"), F.col("nome_empresa"), F.lit("")),
        F.coalesce(F.col("competencia").cast("string"), F.lit("")),
        F.coalesce(F.col("seq").cast("string"), F.lit("")),
    ).orderBy(
        F.col("vl_base_inss").desc_nulls_last(),
        F.col("dt_inicio").asc_nulls_last(),
        F.col("ingested_at_origem").desc_nulls_last(),
        F.col("arquivo_origem").desc_nulls_last(),
    )

    w_id_inss = Window.orderBy(
        F.coalesce(F.col("nu_cpf"), F.lit("")),
        F.coalesce(F.col("nu_cnpj_empresa"), F.lit("")),
        F.coalesce(F.col("nome_empresa"), F.lit("")),
        F.coalesce(F.col("competencia").cast("string"), F.lit("")),
        F.coalesce(F.col("seq").cast("string"), F.lit("")),
    )

    df_final = (
        df_final
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("id_inss", F.row_number().over(w_id_inss))
        .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            F.col("id_inss").cast("int").alias("id_inss"),
            F.col("nu_cpf").cast("string").alias("nu_cpf"),
            F.col("dt_nascimento").cast("date").alias("dt_nascimento"),
            F.col("seq").cast("int").alias("seq"),
            F.col("nome_empresa").cast("string").alias("nome_empresa"),
            F.col("nu_cnpj_empresa").cast("string").alias("nu_cnpj_empresa"),
            F.col("tp_filiado").cast("string").alias("tp_filiado"),
            F.col("nit").cast("string").alias("nit"),
            F.col("nr_pis_pasep").cast("string").alias("nr_pis_pasep"),
            F.col("dt_inicio").cast("date").alias("dt_inicio"),
            F.col("dt_fim").cast("date").alias("dt_fim"),
            F.col("competencia").cast("date").alias("competencia"),
            F.col("vl_base_inss").cast("double").alias("vl_base_inss"),
            F.col("vl_contribuicao_inss").cast("double").alias("vl_contribuicao_inss"),
            F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
            F.col("ingested_at_origem").cast("timestamp").alias("ingested_at_origem"),
            F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
            F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        )
    )

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_INSS, catalog_schema=f"{CATALOG}.{SILVER}")
