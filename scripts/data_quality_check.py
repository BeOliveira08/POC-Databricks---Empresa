#!/usr/bin/env python3
"""
data_quality_check.py

Runs data quality checks on Databricks tables affected by the current PR
and posts a markdown comment to the pull request.

Checks per table:
  - Row count
  - Duplicate rows (on declared PK column)
  - Null values on PK column

Environment variables required:
  DATABRICKS_HOST      - e.g. https://adb-<workspace-id>.<region>.azuredatabricks.net
  DATABRICKS_TOKEN     - personal access token
  DATABRICKS_WAREHOUSE_ID - SQL Warehouse ID
  GITHUB_TOKEN         - provided automatically by GitHub Actions
  GITHUB_REPOSITORY    - e.g. owner/repo (provided automatically)
  PR_NUMBER            - pull request number
  BASE_REF             - base branch name (e.g. master)
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from typing import Optional

# ---------------------------------------------------------------------------
# Notebook → (table, pk_column) mapping
# pk_column = None means Bronze (raw) — only row_count is checked
# ---------------------------------------------------------------------------
NOTEBOOK_TABLE_MAP: dict[str, tuple[str, Optional[str]]] = {
    "empresas":                     ("portfolio.bronze.empresas",                  None),
    "contratos_geral":             ("portfolio.bronze.contratos_geral",           None),
    "beneficios_empresa":          ("portfolio.bronze.beneficios_empresa",        None),
    "fgts_extratos":               ("portfolio.bronze.fgts_extratos",             None),
    "fgts_jsonl":                  ("portfolio.bronze.fgts_extratos",             None),
    "das_pdf":                     ("portfolio.bronze.das",                       None),
    "S1_Bronze_irpf_pdf":          ("portfolio.bronze.irpf_declaracoes",          None),
    "S1_Bronze_contratos_geral":   ("portfolio.bronze.contratos_geral",            None),
    "S1_Bronze_beneficios_empresa":("portfolio.bronze.beneficios_empresa",         None),
    "S1_Bronze_fgts_extratos":     ("portfolio.bronze.fgts_extratos",              None),
    "S1_Bronze_fgts_jsonl":        ("portfolio.bronze.fgts_extratos",              None),
    "S1_Bronze_das_pdf":           ("portfolio.bronze.das",                        None),
    "funcionarios_xlsx":           ("portfolio.bronze.funcionarios_levantamento",  None),
    "reunioes_contratacao_xlsx":   ("portfolio.bronze.reunioes_contratacao",       None),
    "ctps_pdf":                    ("portfolio.bronze.ctps_pdf",                   None),
    "inss_pdf":                    ("portfolio.bronze.inss_pdf",                   None),
    "inss_jsonl":                  ("portfolio.bronze.inss_pdf",                   None),
    "rescisoes_pdf":               ("portfolio.bronze.rescisoes_pdf",              None),
    "nf_provider_a":                  ("portfolio.bronze.nf_provider_a",                 None),
    "nf_portfolio":                      ("portfolio.bronze.nf_portfolio",                     None),
    "S1_Bronze_ctps_pdf":          ("portfolio.bronze.ctps_vinculos",              None),
    "S1_Bronze_inss_jsonl":        ("portfolio.bronze.inss_pdf",                   None),
    "S1_Bronze_nf_provider_a":        ("portfolio.bronze.nf_provider_a",                 None),
    "S1_Bronze_nf_portfolio":            ("portfolio.bronze.nf_portfolio",                     None),
    "empresa":                     ("portfolio.silver.tb_empresa",                "empresa_id"),
    "vinculo":                     ("portfolio.silver.tb_funcionario_empresa",    "id_funcionario_empresa"),
    "funcionario":                 ("portfolio.silver.tb_funcionario",            "id_funcionario"),
    "ctps":                        ("portfolio.silver.tb_ctps",                   "id_ctps"),
    "reunioes_contratacao":        ("portfolio.silver.tb_reunioes_contratacao",   "id_reuniao_contratacao"),
    "beneficios":                  ("portfolio.silver.tb_beneficios",             "id_beneficio"),
    "rescisoes":                   ("portfolio.silver.tb_rescisoes",              "id_rescisao"),
    "informe_rendimentos":         ("portfolio.silver.tb_informe_rendimentos",    "id_informe_rendimentos"),
    "fato_funcionarios":           ("portfolio.gold.fato_funcionarios",             "id_funcionario_interno"),
    "dim_empresa":                 ("portfolio.gold.dim_empresa",                  "empresa_id"),
    "dim_data":                    ("portfolio.gold.dim_data",                     "sk_data"),
    "dim_pessoa":                  ("portfolio.gold.dim_pessoa",                   "sk_pessoa"),
    "dim_cargo":                   ("portfolio.gold.dim_cargo",                    "sk_cargo"),
    "dim_tipo_contrato":           ("portfolio.gold.dim_tipo_contrato",            "sk_tipo_contrato"),
    "dim_reuniao_contratacao_atributo": ("portfolio.gold.dim_reuniao_contratacao_atributo", "sk_reuniao_atributo"),
    "dim_funcionario_operacao_atributo": ("portfolio.gold.dim_funcionario_operacao_atributo", "sk_funcionario_operacao_atributo"),
    "fato_vinculo_empresa":        ("portfolio.gold.fato_vinculo_empresa",         "sk_vinculo"),
    "bridge_vinculo_mes":          ("portfolio.gold.bridge_vinculo_mes",           "sk_vinculo_mes"),
    "fato_empresa_contrato_resumo": ("portfolio.gold.fato_empresa_contrato_resumo", "empresa_id"),
    "fato_empresa_mensal":         ("portfolio.gold.fato_empresa_mensal",          "sk_empresa_mes"),
    "fato_empresa_indicadores":    ("portfolio.gold.fato_empresa_indicadores",     "sk_empresa_indicador"),
    "fato_reunioes_contratacao":   ("portfolio.gold.fato_reunioes_contratacao",   "sk_reuniao_contratacao"),
    "fato_reunioes_contratacao_mensal": ("portfolio.gold.fato_reunioes_contratacao_mensal", "id_reuniao_contratacao_mensal"),
    "fato_beneficios_empresa":      ("portfolio.gold.fato_beneficios_empresa",     "sk_beneficio_empresa"),
    "fato_rescisoes":               ("portfolio.gold.fato_rescisoes",              "sk_rescisao"),
    "fato_informe_rendimentos":     ("portfolio.gold.fato_informe_rendimentos",    "sk_informe_rendimentos"),
    "irpf":                        ("portfolio.silver.tb_irpf",                   "id_irpf"),
    "inss":                        ("portfolio.silver.tb_inss",                   "id_inss"),
    "fgts":                        ("portfolio.silver.tb_fgts",                   "id_fgts"),
    "das":                         ("portfolio.silver.tb_das",                    "id_das"),
    "notas_fiscais":               ("portfolio.silver.tb_notas_fiscais",          "id_nota_fiscal"),
    "fato_irpf":                   ("portfolio.gold.fato_irpf",                   "sk_irpf"),
    "fato_inss":                   ("portfolio.gold.fato_inss",                   "sk_inss"),
    "fato_inss_projecao":          ("portfolio.gold.fato_inss_projecao",          "sk_inss_projecao"),
    "fato_fgts":                   ("portfolio.gold.fato_fgts",                   "sk_fgts"),
    "fato_das":                    ("portfolio.gold.fato_das",                    "sk_das"),
    "fato_notas_fiscais":          ("portfolio.gold.fato_notas_fiscais",          "sk_nota_fiscal"),
    "validate_relationships":       ("portfolio.gold.fato_vinculo_empresa",        "sk_vinculo"),
}

POLL_INTERVAL_S = 5
POLL_TIMEOUT_S = 120


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_changed_notebooks(base_ref: str) -> list[str]:
    """Return stem names of notebooks changed vs base_ref."""
    result = subprocess.run(
        ["git", "diff", f"origin/{base_ref}...HEAD", "--name-only"],
        capture_output=True, text=True,
    )
    stems = []
    for path in result.stdout.splitlines():
        if "pipelines/notebooks/" in path and path.endswith(".py"):
            stem = re.sub(r"\.py$", "", path.split("/")[-1])
            if stem not in ("S1_Bronze_run_all", "S2_Silver_run_all", "S3_Gold_run_all"):
                stems.append(stem)
    return stems


def resolve_affected_tables(stems: list[str]) -> dict[str, tuple[str, Optional[str]]]:
    """Map notebook stems to (table, pk_col), deduplicating by table name."""
    seen: dict[str, tuple[str, Optional[str]]] = {}
    for stem in stems:
        if stem in NOTEBOOK_TABLE_MAP:
            table, pk = NOTEBOOK_TABLE_MAP[stem]
            if table not in seen:
                seen[table] = (table, pk)
    return seen


def _databricks_request(path: str, payload: dict) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    url = f"{host}{path}"
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Databricks API HTTP {e.code} on POST {path}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Databricks API unreachable on POST {path}: {e.reason}") from e


def _databricks_get(path: str) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    url = f"{host}{path}"
    req = urllib.request.Request(
        url, method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Databricks API HTTP {e.code} on GET {path}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Databricks API unreachable on GET {path}: {e.reason}") from e


def run_sql(statement: str) -> Optional[list]:
    """Execute SQL via Statement API and return rows, or None on error."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        return None

    resp = _databricks_request("/api/2.0/sql/statements", {
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "0s",  # async
        "on_wait_timeout": "CONTINUE",
    })

    statement_id = resp.get("statement_id")
    if not statement_id:
        return None

    # Poll until done
    deadline = time.time() + POLL_TIMEOUT_S
    while time.time() < deadline:
        time.sleep(POLL_INTERVAL_S)
        status_resp = _databricks_get(f"/api/2.0/sql/statements/{statement_id}")
        state = status_resp.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            rows = status_resp.get("result", {}).get("data_array", [])
            return rows
        if state in ("FAILED", "CANCELED", "CLOSED"):
            return None

    return None  # timeout


def check_table(table: str, pk_col: Optional[str]) -> dict:
    """Run quality checks on a single table. Returns a result dict."""
    result = {"table": table, "error": None, "row_count": None, "duplicates": None, "null_pk": None}

    if pk_col:
        sql = f"""
SELECT
  COUNT(*) AS row_count,
  COUNT(*) - COUNT(DISTINCT {pk_col}) AS duplicate_rows,
  SUM(CASE WHEN {pk_col} IS NULL THEN 1 ELSE 0 END) AS null_pk
FROM {table}
"""
    else:
        sql = f"SELECT COUNT(*) AS row_count FROM {table}"

    try:
        rows = run_sql(sql.strip())
    except Exception as exc:
        result["error"] = str(exc)
        return result

    if rows is None:
        result["error"] = "query failed or timed out"
        return result

    if not rows:
        result["row_count"] = 0
        return result

    row = rows[0]
    result["row_count"] = int(row[0]) if row[0] is not None else 0
    if pk_col and len(row) >= 3:
        result["duplicates"] = int(row[1]) if row[1] is not None else 0
        result["null_pk"] = int(row[2]) if row[2] is not None else 0

    return result


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------

def build_comment(results: list[dict], warehouse_missing: bool) -> str:
    lines = ["## Data Quality Report\n"]

    if warehouse_missing:
        lines.append("> ⚠️ `DATABRICKS_WAREHOUSE_ID` não configurado — checks de dados ignorados.\n")
        return "\n".join(lines)

    if not results:
        lines.append("Nenhuma tabela afetada por este PR.")
        return "\n".join(lines)

    lines.append("| Tabela | Rows | Duplicatas | Nulls (PK) | Status |")
    lines.append("|---|---|---|---|---|")

    for r in results:
        table = r["table"]
        if r["error"]:
            lines.append(f"| `{table}` | — | — | — | ⚠️ `{r['error']}` |")
            continue

        row_count = r["row_count"] if r["row_count"] is not None else "—"
        dups = r["duplicates"]
        nulls = r["null_pk"]

        if dups is None and nulls is None:
            # Bronze — only row count
            status = "✅" if (r["row_count"] or 0) > 0 else "⚠️ vazia"
            lines.append(f"| `{table}` | {row_count} | — | — | {status} |")
        else:
            status = "✅" if dups == 0 and nulls == 0 else "❌"
            lines.append(f"| `{table}` | {row_count} | {dups} | {nulls} | {status} |")

    lines.append("")
    lines.append("_Snapshot do workspace no momento da abertura do PR._")
    return "\n".join(lines)


def post_pr_comment(body: str) -> None:
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    pr_number = os.environ.get("PR_NUMBER", "")
    token = os.environ.get("GH_TOKEN", os.environ.get("GITHUB_TOKEN", ""))

    if not (repo and pr_number and token):
        print("Skipping PR comment — missing GITHUB_REPOSITORY, PR_NUMBER or GH_TOKEN.")
        return

    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    payload = json.dumps({"body": body}).encode()
    req = urllib.request.Request(
        url, data=payload, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status not in (200, 201):
            print(f"Warning: GitHub comment returned HTTP {resp.status}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    base_ref = os.environ.get("BASE_REF", "master")
    warehouse_missing = not os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()

    stems = get_changed_notebooks(base_ref)
    affected = resolve_affected_tables(stems)

    print(f"Changed notebooks: {stems}")
    print(f"Affected tables: {list(affected.keys())}")

    results = []
    if not warehouse_missing:
        for table, pk_col in affected.values():
            print(f"Checking {table} (pk={pk_col})...")
            results.append(check_table(table, pk_col))

    comment = build_comment(results, warehouse_missing)
    print("\n--- PR Comment ---")
    print(comment)
    print("------------------\n")

    try:
        post_pr_comment(comment)
    except Exception as exc:
        print(f"Warning: failed to post PR comment — {exc}")

    # Fail CI only if there are hard errors (duplicates or nulls)
    has_failures = any(
        (r.get("duplicates") or 0) > 0 or (r.get("null_pk") or 0) > 0
        for r in results
        if not r.get("error")
    )
    return 1 if has_failures else 0


if __name__ == "__main__":
    sys.exit(main())
