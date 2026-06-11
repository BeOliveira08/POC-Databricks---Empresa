#!/usr/bin/env python3
"""
Export all tables from a Gold schema to local CSV files and run basic BI checks
for known tables.

Reads credentials from environment or .env:
  DATABRICKS_HOST
  DATABRICKS_TOKEN
  DATABRICKS_WAREHOUSE_ID

Outputs:
  exports/gold_<catalog>_<schema>/<timestamp>/*.csv
  exports/gold_<catalog>_<schema>/<timestamp>/validation_report.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG = "portfolio_dev"
DEFAULT_SCHEMA = "gold"

TABLES: dict[str, str] = {
    "dim_empresa": "empresa_id",
    "dim_data": "sk_data",
    "dim_pessoa": "sk_pessoa",
    "dim_cargo": "sk_cargo",
    "dim_tipo_contrato": "sk_tipo_contrato",
    "dim_reuniao_contratacao_atributo": "sk_reuniao_atributo",
    "dim_funcionario_operacao_atributo": "sk_funcionario_operacao_atributo",
    "fato_vinculo_empresa": "sk_vinculo",
    "bridge_vinculo_mes": "sk_vinculo_mes",
    "fato_empresa_contrato_resumo": "empresa_id",
    "fato_empresa_mensal": "sk_empresa_mes",
    "fato_empresa_indicadores": "sk_empresa_indicador",
    "fato_funcionarios": "id_funcionario_interno",
    "fato_reunioes_contratacao": "sk_reuniao_contratacao",
    "fato_reunioes_contratacao_mensal": "id_reuniao_contratacao_mensal",
    "fato_fgts": "sk_fgts",
    "fato_inss": "sk_inss",
    "fato_inss_projecao": "sk_inss_projecao",
    "fato_das": "sk_das",
    "fato_notas_fiscais": "sk_nota_fiscal",
    "fato_irpf": "sk_irpf",
}

EMPRESA_FACTS = [
    "fato_vinculo_empresa",
    "bridge_vinculo_mes",
    "fato_empresa_contrato_resumo",
    "fato_empresa_mensal",
    "fato_empresa_indicadores",
    "fato_funcionarios",
    "fato_reunioes_contratacao",
    "fato_fgts",
    "fato_inss",
    "fato_inss_projecao",
    "fato_das",
]

POLL_INTERVAL_S = 2
POLL_TIMEOUT_S = 600


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def api_request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    host = require_env("DATABRICKS_HOST").rstrip("/")
    token = require_env("DATABRICKS_TOKEN")
    url = f"{host}{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Databricks API HTTP {exc.code} on {method} {path}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Databricks API unreachable on {method} {path}: {exc.reason}") from exc


def run_sql(statement: str) -> dict[str, Any]:
    warehouse_id = require_env("DATABRICKS_WAREHOUSE_ID")
    response = api_request(
        "POST",
        "/api/2.0/sql/statements",
        {
            "statement": statement,
            "warehouse_id": warehouse_id,
            "wait_timeout": "10s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        },
    )

    statement_id = response.get("statement_id")
    if not statement_id:
        return response

    deadline = time.time() + POLL_TIMEOUT_S
    current = response
    while time.time() < deadline:
        state = current.get("status", {}).get("state")
        if state == "SUCCEEDED":
            return current
        if state in {"FAILED", "CANCELED", "CLOSED"}:
            message = current.get("status", {}).get("error", {}).get("message", current)
            raise RuntimeError(f"SQL failed: {message}")
        time.sleep(POLL_INTERVAL_S)
        current = api_request("GET", f"/api/2.0/sql/statements/{statement_id}")

    raise TimeoutError(f"SQL timed out after {POLL_TIMEOUT_S}s: {statement[:200]}")


def result_columns(result: dict[str, Any]) -> list[str]:
    schema = result.get("manifest", {}).get("schema", {})
    return [col["name"] for col in schema.get("columns", [])]


def result_rows(result: dict[str, Any]) -> list[list[Any]]:
    return result.get("result", {}).get("data_array", [])


def write_csv(path: Path, columns: list[str], rows: list[list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)


def quote_ident(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def full_table_name(catalog: str, schema: str, name: str) -> str:
    return ".".join(quote_ident(part) for part in (catalog, schema, name))


def scalar(sql: str) -> Any:
    rows = result_rows(run_sql(sql))
    return rows[0][0] if rows and rows[0] else None


def list_tables(catalog: str, schema: str) -> list[str]:
    sql = f"""
    SELECT table_name
    FROM {quote_ident(catalog)}.information_schema.tables
    WHERE table_schema = '{schema.replace("'", "''")}'
      AND table_type = 'BASE TABLE'
    ORDER BY table_name
    """
    rows = result_rows(run_sql(sql))
    return [row[0] for row in rows]


def validate_table(catalog: str, schema: str, name: str, pk: str) -> list[dict[str, Any]]:
    full_name = full_table_name(catalog, schema, name)
    rows: list[dict[str, Any]] = []

    row_count = scalar(f"SELECT COUNT(*) FROM {full_name}")
    null_pk = scalar(f"SELECT COUNT(*) FROM {full_name} WHERE {pk} IS NULL")
    duplicate_pk = scalar(
        f"""
        SELECT COUNT(*)
        FROM (
          SELECT {pk}
          FROM {full_name}
          WHERE {pk} IS NOT NULL
          GROUP BY {pk}
          HAVING COUNT(*) > 1
        )
        """
    )

    rows.append({"table": name, "check": "row_count", "value": row_count, "status": "OK" if int(row_count or 0) > 0 else "WARN"})
    rows.append({"table": name, "check": f"null_pk:{pk}", "value": null_pk, "status": "OK" if int(null_pk or 0) == 0 else "FAIL"})
    rows.append({"table": name, "check": f"duplicate_pk:{pk}", "value": duplicate_pk, "status": "OK" if int(duplicate_pk or 0) == 0 else "FAIL"})

    if name in EMPRESA_FACTS:
        orphan_empresa = scalar(
            f"""
            SELECT COUNT(*)
            FROM {full_name} f
            LEFT JOIN {full_table_name(catalog, schema, "dim_empresa")} d
              ON f.empresa_id = d.empresa_id
            WHERE f.empresa_id IS NOT NULL
              AND d.empresa_id IS NULL
            """
        )
        rows.append(
            {
                "table": name,
                "check": "empresa_id_without_dim_empresa",
                "value": orphan_empresa,
                "status": "OK" if int(orphan_empresa or 0) == 0 else "FAIL",
            }
        )

    return rows


def export_table(catalog: str, schema: str, name: str, output_dir: Path, limit: int | None) -> None:
    full_name = full_table_name(catalog, schema, name)
    sql = f"SELECT * FROM {full_name}"
    if limit is not None:
        sql += f" LIMIT {limit}"

    print(f"Exporting {full_name}...")
    result = run_sql(sql)
    columns = result_columns(result)
    rows = result_rows(result)
    write_csv(output_dir / f"{name}.csv", columns, rows)
    print(f"  wrote {len(rows)} rows")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit per exported table.")
    parser.add_argument("--skip-export", action="store_true", help="Only generate validation_report.csv.")
    parser.add_argument("--tables", nargs="*", help="Optional table names. Default: all base tables in the schema.")
    args = parser.parse_args(argv)

    load_dotenv(ROOT / ".env")
    output_root = args.output_root or ROOT / "exports" / f"gold_{args.catalog}_{args.schema}"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = output_root / timestamp
    output_dir.mkdir(parents=True, exist_ok=True)

    table_names = args.tables if args.tables else list_tables(args.catalog, args.schema)
    if not table_names:
        raise RuntimeError(f"No base tables found in {args.catalog}.{args.schema}")

    print(f"Catalog/schema: {args.catalog}.{args.schema}")
    print(f"Output: {output_dir}")
    print(f"Tables: {', '.join(table_names)}")

    validation_rows: list[dict[str, Any]] = []
    for name in table_names:
        pk = TABLES.get(name)
        if pk:
            validation_rows.extend(validate_table(args.catalog, args.schema, name, pk))
        else:
            row_count = scalar(f"SELECT COUNT(*) FROM {full_table_name(args.catalog, args.schema, name)}")
            validation_rows.append(
                {"table": name, "check": "row_count", "value": row_count, "status": "OK" if int(row_count or 0) > 0 else "WARN"}
            )
        if not args.skip_export:
            export_table(args.catalog, args.schema, name, output_dir, args.limit)

    report_columns = ["table", "check", "value", "status"]
    report_rows = [[row[col] for col in report_columns] for row in validation_rows]
    write_csv(output_dir / "validation_report.csv", report_columns, report_rows)
    print(f"Wrote validation report: {output_dir / 'validation_report.csv'}")

    failures = [row for row in validation_rows if row["status"] == "FAIL"]
    if failures:
        print("Validation failed:")
        for row in failures:
            print(f"  {row['table']} | {row['check']} = {row['value']}")
        return 1

    print("Validation checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
