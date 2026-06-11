# Databricks notebook source
# delta
#
# Reusable Spark/Delta helpers shared across notebooks.
# Usage: %run ../../utils/delta

import time


def table_exists(table_name: str) -> bool:
    """Check if a table exists in the Spark catalog."""
    return spark.catalog.tableExists(table_name)


def existing_columns(available_cols: set, *candidates):
    """Return list of candidate column names that exist in available_cols set."""
    return [col_name for col_name in candidates if col_name in available_cols]


def empty_df_for_schema(schema_ddl: str):
    """Create an empty DataFrame with a given schema DDL string."""
    return spark.createDataFrame([], schema_ddl)


def rename_column_if_needed(table_name: str, old_name: str, new_name: str) -> None:
    """Rename a column only if old name exists and new name doesn't."""
    existing = {f.name for f in spark.table(table_name).schema.fields}
    if old_name in existing and new_name not in existing:
        spark.sql(f"ALTER TABLE {table_name} RENAME COLUMN {old_name} TO {new_name}")
        print(f"[OK] Renamed {old_name} -> {new_name} in {table_name}")


def merge_into(target: str, source_df, merge_key: str | list[str]) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    if isinstance(merge_key, list):
        condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
        key_label = merge_key
        key_cols  = set(merge_key)
    else:
        condition = f"t.{merge_key} = s.{merge_key}"
        key_label = merge_key
        key_cols  = {merge_key}

    if not spark.catalog.tableExists(target):
        source_df.write.format("delta").saveAsTable(target)
        print(f"[OK] merge_into {target} — created on first run")
        return

    target_cols = {f.name for f in spark.table(target).schema.fields}

    if (
        "id_empresa" in target_cols and
        "id_empresa" not in source_df.columns and
        "nu_cnpj" in source_df.columns
    ):
        source_df = source_df.withColumn(
            "id_empresa",
            F.when(
                F.length(F.regexp_replace(F.coalesce(F.col("nu_cnpj").cast("string"), F.lit("")), r"\D", "")) == 14,
                F.regexp_replace(F.col("nu_cnpj").cast("string"), r"\D", "").cast("bigint"),
            ).otherwise(F.lit(None).cast("bigint"))
        )

    source_cols = source_df.columns
    update_map  = {c: f"s.{c}" for c in source_cols if c not in key_cols}
    insert_map  = {c: f"s.{c}" for c in source_cols}

    dt = DeltaTable.forName(spark, target)
    (
        dt.alias("t")
        .merge(source_df.alias("s"), condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    print(f"[OK] merge_into {target} on {key_label}")


def write_overwrite(
    df,
    table_name: str,
    *,
    catalog_schema: str | None = None,
    max_attempts: int = 4,
    retry_sleep_seconds: float = 2.0,
) -> None:
    if catalog_schema:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")

    for attempt in range(1, max_attempts + 1):
        try:
            df.writeTo(table_name).using("delta").createOrReplace()
            print(f"[OK] {table_name} recriada.")
            return
        except Exception as exc:
            message = str(exc).lower()
            is_metadata_race = (
                "delta_metadata_changed" in message
                or "metadatachangedexception" in message
                or "metadata of the delta table has been changed by a concurrent update" in message
            )
            if (not is_metadata_race) or attempt == max_attempts:
                raise

            sleep_seconds = retry_sleep_seconds * attempt
            print(
                f"[WARN] Conflito de metadata ao recriar {table_name} "
                f"(tentativa {attempt}/{max_attempts}). Novo retry em {sleep_seconds:.1f}s."
            )
            time.sleep(sleep_seconds)


def optimize_table(table_name: str, zorder_cols: list | None = None) -> None:
    """Run OPTIMIZE (and optionally ZORDER) on a Delta table.

    Call after large writes to compact small files and improve read performance.
    """
    if zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        print(f"[OK] OPTIMIZE {table_name} ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE {table_name}")
        print(f"[OK] OPTIMIZE {table_name}")
