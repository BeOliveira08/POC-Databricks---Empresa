# Databricks notebook source
# gold: vw_resumo_previdenciario_atual

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../config/domain_config

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

SILVER_TB_INSS = f"{CATALOG}.{SILVER}.tb_inss"
GOLD_DIM_PESSOA = f"{CATALOG}.{GOLD}.dim_pessoa"
GOLD_VW_RESUMO_PREVIDENCIARIO_ATUAL = f"{CATALOG}.{GOLD}.vw_resumo_previdenciario_atual"

if not table_exists(SILVER_TB_INSS):
    raise ValueError(f"Tabela obrigatoria nao encontrada: {SILVER_TB_INSS}")
if not table_exists(GOLD_DIM_PESSOA):
    raise ValueError(f"Dimensao obrigatoria nao encontrada: {GOLD_DIM_PESSOA}")

spark.sql(f"""
CREATE OR REPLACE VIEW {GOLD_VW_RESUMO_PREVIDENCIARIO_ATUAL} AS
WITH pessoa_titular AS (
    SELECT CAST(id_pessoa AS BIGINT) AS id_pessoa
    FROM {GOLD_DIM_PESSOA}
    WHERE id_pessoa = CAST(REGEXP_REPLACE('{TITULAR_CPF}', '[^0-9]', '') AS BIGINT)
    LIMIT 1
),
inss_base AS (
    SELECT
        p.id_pessoa,
        CAST(i.dt_nascimento AS DATE) AS dt_nascimento,
        CAST(i.competencia AS DATE) AS competencia,
        COALESCE(CAST(i.vl_contribuicao_inss AS DOUBLE), 0.0) AS vl_contribuicao_inss,
        CAST(i.vl_base_inss AS DOUBLE) AS vl_base_inss
    FROM {SILVER_TB_INSS} i
    CROSS JOIN pessoa_titular p
    WHERE i.nu_cpf = '{TITULAR_CPF}'
      AND CAST(i.competencia AS DATE) IS NOT NULL
      AND CAST(i.competencia AS DATE) <= current_date()
),
media_12m AS (
    SELECT
        id_pessoa,
        ROUND(AVG(vl_base_inss), 2) AS vl_remuneracao_media_12m
    FROM (
        SELECT
            id_pessoa,
            vl_base_inss,
            ROW_NUMBER() OVER (PARTITION BY id_pessoa ORDER BY competencia DESC NULLS LAST) AS rn_12m
        FROM inss_base
        WHERE vl_contribuicao_inss > 0
    ) x
    WHERE rn_12m <= 12
    GROUP BY id_pessoa
),
resumo AS (
    SELECT
        id_pessoa,
        COALESCE(MAX(dt_nascimento), TO_DATE('{TITULAR_DT_NASCIMENTO}')) AS dt_nascimento,
        MIN(competencia) AS primeira_competencia,
        MAX(competencia) AS ultima_competencia,
        COUNT(DISTINCT competencia) AS qt_meses_contribuidos,
        ROUND(SUM(vl_contribuicao_inss), 2) AS vl_contribuicao_total,
        ROUND(AVG(vl_contribuicao_inss), 2) AS vl_contribuicao_media_mensal,
        ROUND(AVG(vl_base_inss), 2) AS vl_remuneracao_media_total
    FROM inss_base
    WHERE vl_contribuicao_inss > 0
    GROUP BY id_pessoa
)
SELECT
    CAST(r.id_pessoa AS BIGINT) AS id_pessoa,
    CAST(r.primeira_competencia AS DATE) AS primeira_competencia,
    CAST(r.ultima_competencia AS DATE) AS ultima_competencia,
    CAST(r.dt_nascimento AS DATE) AS dt_nascimento,
    CAST(FLOOR(months_between(current_date(), r.dt_nascimento) / 12) AS INT) AS idade_atual_anos,
    CAST(r.qt_meses_contribuidos AS BIGINT) AS qt_meses_contribuidos,
    CAST(GREATEST(0, 420 - r.qt_meses_contribuidos) AS INT) AS qt_meses_faltantes_contribuicao,
    CAST(GREATEST(0, 780 - FLOOR(months_between(current_date(), r.dt_nascimento))) AS INT) AS qt_meses_faltantes_idade,
    CAST(
        GREATEST(
            GREATEST(0, 420 - r.qt_meses_contribuidos),
            GREATEST(0, 780 - FLOOR(months_between(current_date(), r.dt_nascimento)))
        ) AS INT
    ) AS qt_meses_faltantes_aposentadoria,
    CAST(
        add_months(
            current_date(),
            GREATEST(
                GREATEST(0, 420 - r.qt_meses_contribuidos),
                GREATEST(0, 780 - FLOOR(months_between(current_date(), r.dt_nascimento)))
            )
        ) AS DATE
    ) AS dt_aposentadoria_estimada,
    CAST(r.vl_contribuicao_total AS DOUBLE) AS vl_contribuicao_total,
    CAST(r.vl_contribuicao_media_mensal AS DOUBLE) AS vl_contribuicao_media_mensal,
    CAST(r.vl_remuneracao_media_total AS DOUBLE) AS vl_remuneracao_media_total,
    CAST(m.vl_remuneracao_media_12m AS DOUBLE) AS vl_remuneracao_media_12m,
    CAST(
        ROUND(
            LEAST(
                100.0,
                60.0 + GREATEST(0.0, ((r.qt_meses_contribuidos - 240) / 12.0) * 2.0)
            ),
            2
        ) AS DOUBLE
    ) AS pc_fator_beneficio_estimado,
    CAST(
        ROUND(
            COALESCE(m.vl_remuneracao_media_12m, r.vl_remuneracao_media_total, 0.0)
            * (
                ROUND(
                    LEAST(
                        100.0,
                        60.0 + GREATEST(0.0, ((r.qt_meses_contribuidos - 240) / 12.0) * 2.0)
                    ),
                    2
                ) / 100.0
            ),
            2
        ) AS DOUBLE
    ) AS vl_beneficio_estimado_mensal
FROM resumo r
LEFT JOIN media_12m m
  ON r.id_pessoa = m.id_pessoa
""")

# COMMAND ----------
# MAGIC %run ../../data_governance/catalog_metadata

# COMMAND ----------

apply_governance(GOLD_VW_RESUMO_PREVIDENCIARIO_ATUAL)
