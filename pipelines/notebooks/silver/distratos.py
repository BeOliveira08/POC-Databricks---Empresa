# Databricks notebook source
# silver: tb_distratos

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/transforms

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------
# MAGIC %run ../../utils/company

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_RAW_DISTRATOS = f"{CATALOG}.{BRONZE}.raw_distratos"
SILVER_TB_CTPS = f"{CATALOG}.{SILVER}.tb_ctps"
SILVER_TB_CONTRATOS = f"{CATALOG}.{SILVER}.tb_contratos"
SILVER_TB_EMPRESA = f"{CATALOG}.{SILVER}.tb_empresa"
SILVER_TB_NOTAS_FISCAIS = f"{CATALOG}.{SILVER}.tb_notas_fiscais"
SILVER_TB_PROPOSTAS = f"{CATALOG}.{SILVER}.tb_propostas"
SILVER_TB_RESCISOES = f"{CATALOG}.{SILVER}.tb_rescisoes"
SILVER_TB_DISTRATOS = f"{CATALOG}.{SILVER}.tb_distratos"

date_udf = F.udf(parse_flexible_date_to_iso, "string")
resolve_empresa_nome_udf = F.udf(lambda nome, cnpj: resolve_empresa_alias(nome, cnpj)[0], "string")

if not table_exists(BRONZE_RAW_DISTRATOS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {BRONZE_RAW_DISTRATOS}")


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
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\b\d{6,}\b", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9]", " ")
    txt = F.regexp_replace(txt, r"\b(SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU)\b", " ")
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt)


def company_anchor_key_expr(col_expr):
    txt = F.upper(F.coalesce(col_expr.cast("string"), F.lit("")))
    txt = F.regexp_replace(txt, r"\([^)]*\)", " ")
    txt = F.regexp_replace(txt, r"[^A-Z0-9 ]", " ")
    txt = F.regexp_replace(
        txt,
        (
            r"\b("
            r"SA|S A|LTDA|LTD|ME|MEI|EIRELI|EPP|SS|SLU|"
            r"DO|DA|DE|DAS|DOS|E|EM|"
            r"CONSULTORIA|CONSULTING|ASSESSORIA|SERVICOS|SERVICO|"
            r"SOLUCOES|SOLUCAO|SOLUTIONS|TECNOLOGIA|TECNOLOGIAS|INFORMATICA|"
            r"INFORMACAO|COMUNICACAO|SOFTWARE|DESENVOLVIMENTO|SISTEMAS|"
            r"NEGOCIOS|OUTSOURCING|RECURSOS|HUMANOS|PESSOAS|BUSINESS|"
            r"CENTER|ENGENHARIA|TREINAMENTOS|COMERCIO|INTELIGENCIA|DADOS"
            r")\b"
        ),
        " ",
    )
    txt = F.regexp_replace(txt, r"\b[A-Z0-9]\b", " ")
    txt = F.regexp_replace(txt, r"\s+", " ")
    txt = F.trim(txt)
    txt = F.regexp_replace(txt, r"\s+", "")
    return F.when(txt != "", txt).otherwise(name_key_expr(col_expr))


def canonical_company_expr(nome_col: str, cnpj_col: str | None = None):
    cnpj_expr = F.col(cnpj_col).cast("string") if cnpj_col else F.lit(None).cast("string")
    resolved = resolve_empresa_nome_udf(F.col(nome_col).cast("string"), cnpj_expr)
    return F.when(
        resolved.isNull() | (F.trim(resolved) == "") | (resolved == F.lit("NAO_IDENTIFICADA")),
        F.lit(None).cast("string"),
    ).otherwise(resolved)


def parse_date_expr(col_name: str):
    return F.to_date(date_udf(F.col(col_name).cast("string")), "yyyy-MM-dd")


def date_similarity_score(raw_col, ref_col):
    diff = F.abs(F.datediff(raw_col, ref_col))
    return (
        F.when(raw_col.isNull() | ref_col.isNull(), F.lit(0))
         .when(diff <= 7, F.lit(40))
         .when(diff <= 31, F.lit(30))
         .when(diff <= 93, F.lit(20))
         .when(diff <= 366, F.lit(10))
         .otherwise(F.lit(0))
    )


def date_in_window_score(raw_col, start_col, end_col):
    end_filled = F.coalesce(end_col, start_col)
    return (
        F.when(raw_col.isNull() | start_col.isNull(), F.lit(0))
         .when((raw_col >= start_col) & (raw_col <= end_filled), F.lit(20))
         .otherwise(F.lit(0))
    )


def build_reference_frame(df_ref, ref_source: str, source_priority: int, is_strong_date_ref: int):
    return (
        df_ref
        .withColumn("ref_source", F.lit(ref_source))
        .withColumn("source_priority", F.lit(source_priority))
        .withColumn("is_strong_date_ref", F.lit(is_strong_date_ref))
        .withColumn("empresa_ref", F.coalesce(F.col("empresa_ref"), F.lit(None).cast("string")))
        .withColumn("cnpj_ref", normalize_cnpj_expr(only_digits_expr("cnpj_ref")))
        .withColumn("dt_inicio_ref", F.col("dt_inicio_ref").cast("date"))
        .withColumn("dt_fim_ref", F.col("dt_fim_ref").cast("date"))
        .filter(F.col("empresa_ref").isNotNull() | F.col("cnpj_ref").isNotNull())
        .withColumn("empresa_ref_key", name_key_expr(F.col("empresa_ref")))
        .withColumn("empresa_ref_anchor_key", company_anchor_key_expr(F.col("empresa_ref")))
        .dropDuplicates(
            [
                "ref_source",
                "empresa_ref",
                "cnpj_ref",
                "dt_inicio_ref",
                "dt_fim_ref",
                "empresa_ref_key",
                "empresa_ref_anchor_key",
            ]
        )
    )


df_ref_frames = []

if table_exists(SILVER_TB_CONTRATOS):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_CONTRATOS)
            .select(
                canonical_company_expr("consultoria", "cnpj_consultoria").alias("empresa_ref"),
                F.col("cnpj_consultoria").cast("string").alias("cnpj_ref"),
                F.col("dt_inicio").cast("date").alias("dt_inicio_ref"),
                F.col("dt_fim").cast("date").alias("dt_fim_ref"),
            ),
            "CONTRATOS",
            100,
            1,
        )
    )

if table_exists(SILVER_TB_CTPS):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_CTPS)
            .select(
                canonical_company_expr("nome_empresa", "nu_cnpj_empresa").alias("empresa_ref"),
                F.col("nu_cnpj_empresa").cast("string").alias("cnpj_ref"),
                F.col("dt_inicio").cast("date").alias("dt_inicio_ref"),
                F.col("dt_fim").cast("date").alias("dt_fim_ref"),
            ),
            "CTPS",
            90,
            1,
        )
    )

if table_exists(SILVER_TB_RESCISOES):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_RESCISOES)
            .select(
                canonical_company_expr("nome_empresa", "cnpj").alias("empresa_ref"),
                F.col("cnpj").cast("string").alias("cnpj_ref"),
                F.lit(None).cast("date").alias("dt_inicio_ref"),
                F.col("dt_rescisao").cast("date").alias("dt_fim_ref"),
            ),
            "RESCISOES",
            95,
            1,
        )
    )

if table_exists(SILVER_TB_PROPOSTAS):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_PROPOSTAS)
            .select(
                canonical_company_expr("empresa", "cnpj").alias("empresa_ref"),
                F.col("cnpj").cast("string").alias("cnpj_ref"),
                F.col("dt_inicio_prevista").cast("date").alias("dt_inicio_ref"),
                F.lit(None).cast("date").alias("dt_fim_ref"),
            ),
            "PROPOSTAS",
            55,
            0,
        )
    )

if table_exists(SILVER_TB_NOTAS_FISCAIS):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_NOTAS_FISCAIS)
            .select(
                canonical_company_expr("tomador_nome", "tomador_cnpj").alias("empresa_ref"),
                F.col("tomador_cnpj").cast("string").alias("cnpj_ref"),
                F.coalesce(F.col("competencia"), F.col("data_emissao")).cast("date").alias("dt_ref"),
            )
            .filter(F.col("dt_ref").isNotNull())
            .groupBy("empresa_ref", "cnpj_ref")
            .agg(
                F.min("dt_ref").alias("dt_inicio_ref"),
                F.max("dt_ref").alias("dt_fim_ref"),
            ),
            "NOTAS_FISCAIS",
            35,
            0,
        )
    )

if table_exists(SILVER_TB_EMPRESA):
    df_ref_frames.append(
        build_reference_frame(
            spark.table(SILVER_TB_EMPRESA)
            .select(
                canonical_company_expr("nm_empresa", "nu_cnpj").alias("empresa_ref"),
                F.col("nu_cnpj").cast("string").alias("cnpj_ref"),
                F.lit(None).cast("date").alias("dt_inicio_ref"),
                F.lit(None).cast("date").alias("dt_fim_ref"),
            ),
            "EMPRESA",
            20,
            0,
        )
    )

if df_ref_frames:
    df_ref = df_ref_frames[0]
    for df_next in df_ref_frames[1:]:
        df_ref = df_ref.unionByName(df_next)
else:
    df_ref = spark.createDataFrame(
        [],
        """
        empresa_ref string,
        cnpj_ref string,
        dt_inicio_ref date,
        dt_fim_ref date,
        ref_source string,
        source_priority int,
        is_strong_date_ref int,
        empresa_ref_key string,
        empresa_ref_anchor_key string
        """,
    )


df_base = (
    spark.table(BRONZE_RAW_DISTRATOS)
    .select(
        nullable_text_expr("empresa").alias("empresa_extraida"),
        normalize_cnpj_expr(only_digits_expr("cnpj")).alias("cnpj"),
        parse_date_expr("dt_inicio").alias("dt_inicio"),
        parse_date_expr("dt_fim").alias("dt_fim"),
        norm_text("motivo").alias("motivo"),
        F.coalesce(F.col("arquivo_origem_txt"), F.col("arquivo_pdf_validacao")).cast("string").alias("arquivo_origem"),
    )
    .filter(F.col("empresa_extraida").isNotNull() | F.col("cnpj").isNotNull())
    .withColumn(
        "empresa",
        F.coalesce(
            canonical_company_expr("empresa_extraida", "cnpj"),
            F.col("empresa_extraida"),
        )
    )
    .withColumn("empresa_key", name_key_expr(F.col("empresa")))
    .withColumn("empresa_anchor_key", company_anchor_key_expr(F.col("empresa")))
    .withColumn(
        "dt_inicio",
        F.when(F.col("dt_inicio") <= F.lit("1900-01-01").cast("date"), F.lit(None).cast("date"))
         .when(F.year(F.col("dt_inicio")) < 2000, F.lit(None).cast("date"))
         .otherwise(F.col("dt_inicio"))
    )
    .withColumn(
        "dt_fim",
        F.when(F.col("dt_fim") <= F.lit("1900-01-01").cast("date"), F.lit(None).cast("date"))
         .when(F.year(F.col("dt_fim")) < 2000, F.lit(None).cast("date"))
         .otherwise(F.col("dt_fim"))
    )
    .withColumn("distrato_match_id", F.monotonically_increasing_id())
)


candidate_frames = []

if df_ref.columns:
    candidate_frames.append(
        df_base.alias("d")
        .join(
            F.broadcast(df_ref.alias("r")),
            F.col("d.cnpj") == F.col("r.cnpj_ref"),
            "inner",
        )
        .withColumn("match_level", F.lit(300))
    )
    candidate_frames.append(
        df_base.alias("d")
        .join(
            F.broadcast(df_ref.alias("r")),
            F.col("d.empresa_key") == F.col("r.empresa_ref_key"),
            "inner",
        )
        .withColumn("match_level", F.lit(200))
    )
    candidate_frames.append(
        df_base.alias("d")
        .join(
            F.broadcast(df_ref.alias("r")),
            F.col("d.empresa_anchor_key") == F.col("r.empresa_ref_anchor_key"),
            "inner",
        )
        .withColumn("match_level", F.lit(100))
    )

if candidate_frames:
    df_candidates = candidate_frames[0]
    for df_next in candidate_frames[1:]:
        df_candidates = df_candidates.unionByName(df_next)

    df_candidates = (
        df_candidates
        .withColumn(
            "company_conflict_penalty",
            F.when(
                (F.col("match_level") == 300)
                & F.col("d.empresa_anchor_key").isNotNull()
                & F.col("r.empresa_ref_anchor_key").isNotNull()
                & (F.col("d.empresa_anchor_key") != F.col("r.empresa_ref_anchor_key")),
                F.lit(220),
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "candidate_score",
            F.col("match_level")
            + F.col("r.source_priority")
            + date_similarity_score(F.col("d.dt_inicio"), F.col("r.dt_inicio_ref"))
            + date_similarity_score(F.col("d.dt_fim"), F.col("r.dt_fim_ref"))
            + date_in_window_score(F.col("d.dt_inicio"), F.col("r.dt_inicio_ref"), F.col("r.dt_fim_ref"))
            + date_in_window_score(F.col("d.dt_fim"), F.col("r.dt_inicio_ref"), F.col("r.dt_fim_ref"))
            + F.when(F.col("d.dt_inicio").isNull() & F.col("r.dt_inicio_ref").isNotNull(), F.lit(5)).otherwise(F.lit(0))
            + F.when(F.col("d.dt_fim").isNull() & F.col("r.dt_fim_ref").isNotNull(), F.lit(5)).otherwise(F.lit(0))
            - F.col("company_conflict_penalty")
        )
        .select(
            F.col("d.distrato_match_id").alias("distrato_match_id"),
            F.col("r.empresa_ref").alias("empresa_ref"),
            F.col("r.cnpj_ref").alias("cnpj_ref"),
            F.col("r.dt_inicio_ref").alias("dt_inicio_ref"),
            F.col("r.dt_fim_ref").alias("dt_fim_ref"),
            F.col("r.ref_source").alias("ref_source"),
            F.col("r.source_priority").alias("source_priority"),
            F.col("r.is_strong_date_ref").alias("is_strong_date_ref"),
            F.col("match_level").alias("match_level"),
            F.col("candidate_score").alias("candidate_score"),
        )
        .dropDuplicates(
            [
                "distrato_match_id",
                "empresa_ref",
                "cnpj_ref",
                "dt_inicio_ref",
                "dt_fim_ref",
                "ref_source",
                "match_level",
            ]
        )
    )
else:
    df_candidates = spark.createDataFrame(
        [],
        """
        distrato_match_id long,
        empresa_ref string,
        cnpj_ref string,
        dt_inicio_ref date,
        dt_fim_ref date,
        ref_source string,
        source_priority int,
        is_strong_date_ref int,
        match_level int,
        candidate_score int
        """,
    )


w_candidate = Window.partitionBy("distrato_match_id").orderBy(
    F.col("candidate_score").desc_nulls_last(),
    F.col("match_level").desc_nulls_last(),
    F.col("is_strong_date_ref").desc_nulls_last(),
    F.col("source_priority").desc_nulls_last(),
    F.col("dt_fim_ref").desc_nulls_last(),
    F.col("dt_inicio_ref").desc_nulls_last(),
    F.col("empresa_ref").asc_nulls_last(),
)

df_best_ref_any = (
    df_candidates
    .withColumn("rn", F.row_number().over(w_candidate))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("distrato_match_id"),
        F.col("empresa_ref").alias("empresa_ref_any"),
        F.col("cnpj_ref").alias("cnpj_ref_any"),
        F.col("dt_inicio_ref").alias("dt_inicio_ref_any"),
        F.col("dt_fim_ref").alias("dt_fim_ref_any"),
        F.col("ref_source").alias("ref_source_any"),
        F.col("match_level").alias("match_level_any"),
        F.col("candidate_score").alias("candidate_score_any"),
    )
)

df_best_ref_strong = (
    df_candidates
    .filter(F.col("is_strong_date_ref") == 1)
    .withColumn("rn", F.row_number().over(w_candidate))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("distrato_match_id"),
        F.col("empresa_ref").alias("empresa_ref_strong"),
        F.col("cnpj_ref").alias("cnpj_ref_strong"),
        F.col("dt_inicio_ref").alias("dt_inicio_ref_strong"),
        F.col("dt_fim_ref").alias("dt_fim_ref_strong"),
        F.col("ref_source").alias("ref_source_strong"),
        F.col("match_level").alias("match_level_strong"),
        F.col("candidate_score").alias("candidate_score_strong"),
    )
)


df_final = (
    df_base.alias("d")
    .join(df_best_ref_any.alias("ra"), "distrato_match_id", "left")
    .join(df_best_ref_strong.alias("rs"), "distrato_match_id", "left")
    .withColumn(
        "empresa",
        F.coalesce(
            F.col("ra.empresa_ref_any"),
            F.col("rs.empresa_ref_strong"),
            F.col("d.empresa"),
            F.col("d.empresa_extraida"),
        )
    )
    .withColumn("cnpj_ref_fill", F.coalesce(F.col("rs.cnpj_ref_strong"), F.col("ra.cnpj_ref_any")))
    .withColumn("dt_inicio_ref_fill", F.coalesce(F.col("rs.dt_inicio_ref_strong"), F.col("ra.dt_inicio_ref_any")))
    .withColumn("dt_fim_ref_fill", F.coalesce(F.col("rs.dt_fim_ref_strong"), F.col("ra.dt_fim_ref_any")))
    .withColumn(
        "cnpj",
        F.when(F.col("d.cnpj").isNull(), F.col("cnpj_ref_fill"))
         .when(
             F.col("cnpj_ref_fill").isNotNull()
             & (F.col("d.cnpj") != F.col("cnpj_ref_fill"))
             & (F.coalesce(F.col("ra.match_level_any"), F.lit(0)) >= 100),
             F.col("cnpj_ref_fill"),
         )
         .otherwise(F.col("d.cnpj"))
    )
    .withColumn(
        "dt_inicio_pre",
        F.when(F.col("d.dt_inicio").isNull(), F.col("dt_inicio_ref_fill"))
         .when(F.col("rs.dt_inicio_ref_strong").isNull(), F.col("d.dt_inicio"))
         .when(
             F.col("d.dt_inicio").isNotNull()
             & F.col("d.dt_fim").isNotNull()
             & (F.col("d.dt_inicio") == F.col("d.dt_fim"))
             & F.col("rs.dt_inicio_ref_strong").isNotNull()
             & F.col("rs.dt_fim_ref_strong").isNotNull()
             & (F.col("d.dt_inicio") == F.col("rs.dt_fim_ref_strong"))
             & (F.col("rs.dt_inicio_ref_strong") < F.col("rs.dt_fim_ref_strong")),
             F.col("rs.dt_inicio_ref_strong"),
         )
         .when(
             F.col("rs.dt_inicio_ref_strong").isNotNull()
             & (
                 (F.abs(F.datediff(F.col("d.dt_inicio"), F.col("rs.dt_inicio_ref_strong"))) > 365)
                 | (
                     F.col("rs.dt_fim_ref_strong").isNotNull()
                     & (F.col("d.dt_inicio") > F.date_add(F.col("rs.dt_fim_ref_strong"), 31))
                 )
             ),
             F.col("rs.dt_inicio_ref_strong"),
         )
         .otherwise(F.col("d.dt_inicio"))
    )
    .withColumn(
        "dt_fim_pre",
        F.when(F.col("d.dt_fim").isNull(), F.col("dt_fim_ref_fill"))
         .when(F.col("rs.dt_fim_ref_strong").isNull(), F.col("d.dt_fim"))
         .when(
             F.col("d.dt_inicio").isNull()
             & F.col("rs.dt_inicio_ref_strong").isNotNull()
             & F.col("rs.dt_fim_ref_strong").isNotNull()
             & (F.col("d.dt_fim") == F.col("rs.dt_inicio_ref_strong"))
             & (F.col("rs.dt_fim_ref_strong") > F.col("rs.dt_inicio_ref_strong")),
             F.col("rs.dt_fim_ref_strong"),
         )
         .when(
             F.col("rs.dt_inicio_ref_strong").isNotNull()
             & (F.col("d.dt_fim") < F.col("rs.dt_inicio_ref_strong")),
             F.col("rs.dt_fim_ref_strong"),
         )
         .when(
             F.col("rs.dt_fim_ref_strong").isNotNull()
             & (F.abs(F.datediff(F.col("d.dt_fim"), F.col("rs.dt_fim_ref_strong"))) > 180),
             F.col("rs.dt_fim_ref_strong"),
         )
         .otherwise(F.col("d.dt_fim"))
    )
    .withColumn(
        "dt_inicio",
        F.when(
            F.col("dt_inicio_pre").isNotNull()
            & F.col("dt_fim_pre").isNotNull()
            & (F.col("dt_inicio_pre") > F.col("dt_fim_pre")),
            F.coalesce(
                F.when(
                    F.col("rs.dt_inicio_ref_strong").isNotNull()
                    & (F.col("rs.dt_inicio_ref_strong") <= F.col("dt_fim_pre")),
                    F.col("rs.dt_inicio_ref_strong"),
                ),
                F.when(
                    F.col("dt_inicio_ref_fill").isNotNull()
                    & (F.col("dt_inicio_ref_fill") <= F.col("dt_fim_pre")),
                    F.col("dt_inicio_ref_fill"),
                ),
                F.col("dt_inicio_pre"),
            ),
        ).otherwise(F.col("dt_inicio_pre"))
    )
    .withColumn(
        "dt_fim",
        F.when(
            F.col("dt_fim_pre").isNotNull()
            & F.col("dt_inicio").isNotNull()
            & (F.col("dt_fim_pre") < F.col("dt_inicio")),
            F.coalesce(
                F.when(
                    F.col("rs.dt_fim_ref_strong").isNotNull()
                    & (F.col("rs.dt_fim_ref_strong") >= F.col("dt_inicio")),
                    F.col("rs.dt_fim_ref_strong"),
                ),
                F.when(
                    F.col("dt_fim_ref_fill").isNotNull()
                    & (F.col("dt_fim_ref_fill") >= F.col("dt_inicio")),
                    F.col("dt_fim_ref_fill"),
                ),
                F.lit(None).cast("date"),
            ),
        ).otherwise(F.col("dt_fim_pre"))
    )
    .withColumn(
        "motivo",
        F.when(F.trim(F.coalesce(F.col("d.motivo"), F.lit(""))) == "", F.lit("SEM MOTIVO ESPECIFICADO"))
         .otherwise(F.col("d.motivo"))
    )
    .drop(
        "empresa_extraida",
        "empresa_key",
        "empresa_anchor_key",
        "cnpj_ref_fill",
        "dt_inicio_ref_fill",
        "dt_fim_ref_fill",
        "dt_inicio_pre",
        "dt_fim_pre",
        "empresa_ref_any",
        "cnpj_ref_any",
        "dt_inicio_ref_any",
        "dt_fim_ref_any",
        "ref_source_any",
        "match_level_any",
        "candidate_score_any",
        "empresa_ref_strong",
        "cnpj_ref_strong",
        "dt_inicio_ref_strong",
        "dt_fim_ref_strong",
        "ref_source_strong",
        "match_level_strong",
        "candidate_score_strong",
        "distrato_match_id",
    )
)

w = Window.partitionBy(
    F.coalesce(F.col("cnpj"), F.col("empresa"), F.lit("")),
    F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")),
    F.coalesce(F.col("dt_fim").cast("string"), F.lit("")),
).orderBy(
    F.when(F.col("motivo").isNotNull(), F.lit(0)).otherwise(F.lit(1)),
    F.col("arquivo_origem").desc_nulls_last(),
)

w_id_distrato = Window.orderBy(
    F.coalesce(F.col("cnpj"), F.lit("")),
    F.coalesce(F.col("empresa"), F.lit("")),
    F.coalesce(F.col("dt_inicio").cast("string"), F.lit("")),
    F.coalesce(F.col("dt_fim").cast("string"), F.lit("")),
    F.coalesce(F.col("arquivo_origem"), F.lit("")),
)

df_final = (
    df_final
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("id_distrato", F.row_number().over(w_id_distrato))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID).cast("string"))
    .withColumn("ingested_at", F.current_timestamp())
    .select(
        F.col("id_distrato").cast("int").alias("id_distrato"),
        F.col("empresa").cast("string").alias("empresa"),
        F.col("cnpj").cast("string").alias("cnpj"),
        F.col("dt_inicio").cast("date").alias("dt_inicio"),
        F.col("dt_fim").cast("date").alias("dt_fim"),
        F.col("motivo").cast("string").alias("motivo"),
        F.col("arquivo_origem").cast("string").alias("arquivo_origem"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
    )
)

df_final = fill_silver_nulls(df_final)

write_overwrite(df_final, SILVER_TB_DISTRATOS, catalog_schema=f"{CATALOG}.{SILVER}")
