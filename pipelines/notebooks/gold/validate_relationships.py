# Databricks notebook source
# gold: validate_relationships

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------

from pyspark.sql import functions as F


def full_gold(table_name: str) -> str:
    return f"{CATALOG}.{GOLD}.{table_name}"


RELATIONSHIPS = [
    # fact, fk_col, dim, dim_key, required_not_null
    ("fato_vinculo_empresa", "sk_pessoa", "dim_pessoa", "id_pessoa", True),
    ("fato_vinculo_empresa", "empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_vinculo_empresa", "sk_cargo", "dim_cargo", "id_cargo", False),
    ("fato_vinculo_empresa", "sk_tipo_contrato", "dim_tipo_contrato", "id_tipo_contrato", False),
    ("fato_vinculo_empresa", "sk_data_inicio", "dim_data", "id_data", False),
    ("fato_vinculo_empresa", "sk_data_fim", "dim_data", "id_data", False),

    ("bridge_vinculo_mes", "sk_vinculo", "fato_vinculo_empresa", "sk_vinculo", True),
    ("bridge_vinculo_mes", "sk_pessoa", "dim_pessoa", "id_pessoa", True),
    ("bridge_vinculo_mes", "empresa_id", "dim_empresa", "empresa_id", True),
    ("bridge_vinculo_mes", "sk_cargo", "dim_cargo", "id_cargo", False),
    ("bridge_vinculo_mes", "sk_tipo_contrato", "dim_tipo_contrato", "id_tipo_contrato", False),
    ("bridge_vinculo_mes", "sk_data_mes", "dim_data", "id_data", True),
    ("bridge_vinculo_mes", "sk_data_inicio", "dim_data", "id_data", False),
    ("bridge_vinculo_mes", "sk_data_fim", "dim_data", "id_data", False),

    ("fato_reunioes_contratacao", "empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_reunioes_contratacao", "sk_empresa_contratacao", "dim_empresa_contratacao", "id_empresa_contratacao", False),
    ("fato_reunioes_contratacao", "sk_data_reuniao", "dim_data", "id_data", False),
    ("fato_resultado_contratacao", "sk_empresa_contratacao", "dim_empresa_contratacao", "id_empresa_contratacao", False),
    ("fato_resultado_contratacao", "empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_resultado_contratacao", "sk_data_primeiro_evento", "dim_data", "id_data", False),
    ("fato_resultado_contratacao", "sk_data_ultimo_evento", "dim_data", "id_data", False),

    ("fato_funcionarios", "empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_funcionarios", "sk_funcionario", "dim_pessoa", "id_pessoa", True),

    ("fato_beneficios_empresa", "empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_rescisao_valores", "sk_rescisao", "dim_rescisao", "id_rescisao", True),
    ("fato_rescisao_valores", "sk_pessoa", "dim_pessoa", "id_pessoa", True),
    ("fato_rescisao_valores", "sk_data_rescisao", "dim_data", "id_data", True),
    ("fato_rescisoes", "sk_pessoa", "dim_pessoa", "id_pessoa", False),
    ("fato_rescisoes", "sk_data_rescisao", "dim_data", "id_data", False),
    ("fato_informe_rendimentos", "sk_pessoa", "dim_pessoa", "id_pessoa", False),

    ("fato_fgts", "sk_pessoa", "dim_pessoa", "id_pessoa", True),
    ("fato_fgts", "empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_fgts", "sk_data_competencia", "dim_data", "id_data", True),

    ("fato_inss", "sk_pessoa", "dim_pessoa", "id_pessoa", True),
    ("fato_inss", "empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_inss", "sk_data_inicio", "dim_data", "id_data", False),
    ("fato_inss", "sk_data_fim", "dim_data", "id_data", False),
    ("fato_inss", "sk_data_competencia", "dim_data", "id_data", True),

    ("fato_das", "empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_das", "sk_data_competencia", "dim_data", "id_data", True),
    ("fato_das", "sk_data_vencimento", "dim_data", "id_data", False),
    ("fato_das", "sk_data_arrecadacao", "dim_data", "id_data", False),
    ("fato_das_semestral", "empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_das_semestral", "sk_data_semestre", "dim_data", "id_data", True),

    ("fato_notas_fiscais", "prestador_empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_notas_fiscais", "tomador_empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_notas_fiscais", "sk_data_competencia", "dim_data", "id_data", False),
    ("fato_notas_fiscais", "sk_data_emissao", "dim_data", "id_data", True),

    ("fato_imposto_serv", "prestador_empresa_id", "dim_empresa", "empresa_id", True),
    ("fato_imposto_serv", "tomador_empresa_id", "dim_empresa", "empresa_id", False),
    ("fato_imposto_serv", "sk_data_competencia", "dim_data", "id_data", False),
    ("fato_imposto_serv", "sk_data_emissao", "dim_data", "id_data", True),

    ("fato_irpf", "sk_pessoa", "dim_pessoa", "id_pessoa", True),

    ("fato_reunioes_contratacao_mensal", "sk_data_mes", "dim_data", "id_data", True),
]

SCHEMA_CONTRACTS = {
    "dim_empresa": [
        "empresa_id",
        "nu_cnpj",
        "nm_empresa",
        "tp_empresa",
    ],
    "dim_pessoa": [
        "id_pessoa",
        "nome_pessoa",
        "tipo_pessoa",
    ],
    "fato_vinculo_empresa": [
        "sk_vinculo",
        "sk_pessoa",
        "empresa_id",
        "sk_tipo_contrato",
        "dt_inicio",
    ],
}


results = []

for table_name, required_columns in SCHEMA_CONTRACTS.items():
    full_name = full_gold(table_name)
    if not table_exists(full_name):
        raise ValueError(f"Tabela obrigatoria nao encontrada para contrato de schema: {full_name}")
    existing = set(spark.table(full_name).columns)
    missing = [col for col in required_columns if col not in existing]
    if missing:
        raise RuntimeError(
            f"Quebra de contrato estrutural em {full_name}. Colunas obrigatorias ausentes: {missing}"
        )

for fact, fk_col, dim, dim_key, required_not_null in RELATIONSHIPS:
    fact_table = full_gold(fact)
    dim_table = full_gold(dim)

    if not table_exists(fact_table):
        raise ValueError(f"Tabela fato obrigatoria nao encontrada: {fact_table}")
    if not table_exists(dim_table):
        raise ValueError(f"Dimensao obrigatoria nao encontrada: {dim_table}")

    fact_cols = set(spark.table(fact_table).columns)
    dim_cols = set(spark.table(dim_table).columns)
    if fk_col not in fact_cols:
        raise ValueError(f"FK {fk_col} nao encontrada em {fact_table}")
    if dim_key not in dim_cols:
        raise ValueError(f"Chave {dim_key} nao encontrada em {dim_table}")

    df_fact = spark.table(fact_table).select(F.col(fk_col).alias("fk_value"))
    df_dim = spark.table(dim_table).select(F.col(dim_key).alias("dim_value")).dropDuplicates()

    total_rows = df_fact.count()
    null_fk = df_fact.filter(F.col("fk_value").isNull()).count()
    orphan_fk = (
        df_fact
        .filter(F.col("fk_value").isNotNull())
        .join(df_dim, F.col("fk_value") == F.col("dim_value"), "left_anti")
        .count()
    )

    should_fail = orphan_fk > 0 or (required_not_null and null_fk > 0)

    results.append({
        "fact_table": fact,
        "fk_col": fk_col,
        "dim_table": dim,
        "dim_key": dim_key,
        "required_not_null": required_not_null,
        "total_rows": total_rows,
        "null_fk": null_fk,
        "orphan_fk": orphan_fk,
        "status": "FAIL" if should_fail else ("WARN" if null_fk > 0 else "OK"),
    })

df_results = spark.createDataFrame(results)
display(df_results.orderBy("status", "fact_table", "fk_col"))

failures = [r for r in results if r["status"] == "FAIL"]
if failures:
    lines = [
        f"{r['fact_table']}.{r['fk_col']} -> {r['dim_table']}.{r['dim_key']}: "
        f"null_fk={r['null_fk']} orphan_fk={r['orphan_fk']} required={r['required_not_null']}"
        for r in failures
    ]
    raise RuntimeError("Falha de integridade referencial na gold:\n" + "\n".join(lines))

print("[OK] Integridade referencial gold validada.")
