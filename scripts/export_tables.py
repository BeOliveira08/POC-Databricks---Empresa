# Databricks notebook source
# Exporta todas as tabelas de bronze, silver e gold em CSV nomeado

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------


from pyspark.sql import functions as F
import os

# COMMAND ----------

SCHEMAS = {
    #"bronze": f"{CATALOG}.{BRONZE}",
    #"silver": f"{CATALOG}.{SILVER}",
    "gold": f"{CATALOG}.{GOLD}",
}

#OUTPUT_ROOT = f"{VOL_BASE}/_exports"
OUTPUT_ROOT = f"{VOL_BASE}/_exports_silver"

print("Schemas de origem:")
for camada, schema_name in SCHEMAS.items():
    print(f"  - {camada}: {schema_name}")

print(f"Output root: {OUTPUT_ROOT}")

# COMMAND ----------

def list_tables(schema_name: str):
    return spark.sql(f"SHOW TABLES IN {schema_name}")

def dbfs_to_local_path(dbfs_path: str) -> str:
    if dbfs_path.startswith("dbfs:"):
        return "/dbfs" + dbfs_path[len("dbfs:"):]
    return dbfs_path

def rename_single_csv(output_path: str, final_filename: str):
    local_dir = dbfs_to_local_path(output_path)

    if not os.path.exists(local_dir):
        raise FileNotFoundError(f"Diretorio nao encontrado: {local_dir}")

    files = os.listdir(local_dir)
    csv_files = [f for f in files if f.startswith("part-") and f.endswith(".csv")]

    if not csv_files:
        raise FileNotFoundError(f"Nenhum arquivo CSV part encontrado em: {local_dir}")

    if len(csv_files) > 1:
        raise RuntimeError(
            f"Mais de um part file encontrado em {local_dir}: {csv_files}. "
            "Era esperado apenas 1 arquivo apos coalesce(1)."
        )

    source_file = os.path.join(local_dir, csv_files[0])
    target_file = os.path.join(local_dir, final_filename)

    if os.path.exists(target_file):
        os.remove(target_file)

    os.rename(source_file, target_file)

# COMMAND ----------

resumo = []

for camada, schema_name in SCHEMAS.items():
    print("=" * 100)
    print(f"Processando camada: {camada} | schema: {schema_name}")
    print("=" * 100)

    tabelas = list_tables(schema_name)

    for row in tabelas.collect():
        table_name = row["tableName"]
        full_name = f"{schema_name}.{table_name}"
        output_path = f"{OUTPUT_ROOT}/{camada}/{table_name}"
        final_filename = f"{table_name}.csv"

        print(f"Exportando: {full_name} -> {output_path}/{final_filename}")

        try:
            df = spark.table(full_name)

            (
                df.coalesce(1)
                .write
                .mode("overwrite")
                .option("header", True)
                .csv(output_path)
            )

            rename_single_csv(output_path, final_filename)

            resumo.append((camada, full_name, output_path, "OK"))
            print(f"[OK] {full_name}")

        except Exception as e:
            resumo.append((camada, full_name, output_path, f"ERRO: {str(e)}"))
            print(f"[ERRO] {full_name} -> {e}")

# COMMAND ----------

df_resumo = spark.createDataFrame(
    resumo,
    ["camada", "tabela", "output_path", "status"]
)

display(df_resumo.orderBy("camada", "tabela"))

# COMMAND ----------

qtd_erros = df_resumo.filter(F.col("status").startswith("ERRO")).count()

if qtd_erros > 0:
    raise Exception(f"Exportacao finalizada com {qtd_erros} erro(s). Verifique o resumo.")

print("[OK] Exportacao concluida com sucesso.")