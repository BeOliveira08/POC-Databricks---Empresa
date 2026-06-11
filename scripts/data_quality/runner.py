"""Engine de execução da suite + CLI.

Responsabilidades:
    - Carregar .env
    - Executar cada Check via Databricks SQL Statements API
    - Renderizar relatório (console ou markdown)
    - Sair com exit 0/1 baseado nos resultados

Para adicionar checks novos, edite suite.py — não esta engine.
Para postar resultado no GitHub, use o workflow data-quality-check.yml.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import requests

from .checks import Check, CheckResult


REPO = Path(__file__).resolve().parent.parent.parent
ENV_FILE = REPO / ".env"

CATALOG_BY_TARGET = {"dev": "portfolio_dev", "prod": "portfolio"}


def configure_output_encoding() -> None:
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def load_env(env_file: Path) -> None:
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


class DatabricksSQL:
    def __init__(self, host: str, token: str, warehouse_id: str):
        self.host = host.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}"}
        self.warehouse_id = warehouse_id

    def query_scalar(self, sql: str) -> int:
        url = f"{self.host}/api/2.0/sql/statements"
        body = {"warehouse_id": self.warehouse_id, "statement": sql, "wait_timeout": "50s"}
        r = requests.post(url, json=body, headers=self.headers, timeout=120)
        self._raise_for_status(r)
        data = r.json()
        sid = data["statement_id"]
        state = data["status"]["state"]
        while state in ("PENDING", "RUNNING"):
            time.sleep(2)
            r = requests.get(
                f"{self.host}/api/2.0/sql/statements/{sid}",
                headers=self.headers,
                timeout=60,
            )
            self._raise_for_status(r)
            data = r.json()
            state = data["status"]["state"]
        if state != "SUCCEEDED":
            err = data.get("status", {}).get("error", {}).get("message", str(data))
            raise RuntimeError(f"SQL falhou ({state}): {err}")
        rows = data.get("result", {}).get("data_array") or [[0]]
        return int(rows[0][0])

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            details = response.text.strip()
            if len(details) > 400:
                details = details[:400] + "..."
            raise RuntimeError(
                f"HTTP {response.status_code} ao chamar Databricks SQL API: {details or str(exc)}"
            ) from exc


def run_suite(client: DatabricksSQL, catalog: str, suite: list[Check]) -> list[CheckResult]:
    results = []
    for check in suite:
        display_name = getattr(check, "display_name", "") or ""
        description = getattr(check, "description", "") or ""
        severity = getattr(check, "severity", "error") or "error"
        try:
            sql = check.render_sql(catalog)
            count = client.query_scalar(sql)
            status, detail = check.evaluate(count)
            results.append(CheckResult(
                name=check.name,
                table=check.table,
                status=status,
                violations=count,
                detail=detail,
                severity=severity,
                display_name=display_name,
                description=description,
            ))
        except Exception as exc:
            results.append(CheckResult(
                name=check.name,
                table=check.table,
                status="ERROR",
                violations=0,
                detail=str(exc)[:80],
                severity=severity,
                display_name=display_name,
                description=description,
            ))
    return results


def humanize_check_name(name: str) -> str:
    if " >= " in name and name.endswith(" row(s)"):
        table, min_rows = name.split(" >= ", 1)
        min_rows = min_rows.replace("row(s)", "linha(s)")
        return f"{table}: tem pelo menos {min_rows}"
    if name.endswith(" not null"):
        subject = name.removesuffix(" not null")
        table, _, col = subject.rpartition(".")
        return f"{table}: {col} preenchido" if table else f"{subject} preenchido"
    if name.endswith(" unique"):
        subject = name.removesuffix(" unique")
        table, _, key = subject.rpartition(".")
        if key.startswith("("):
            return f"{table}: chave {key} unica"
        return f"{table}: {key} unico" if table else f"{subject} unico"
    return name.replace("_", " ")


def result_label(result: CheckResult) -> str:
    return result.display_name or humanize_check_name(result.name)


def markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def filter_results(results: list[CheckResult], status: str = "all", severity: str = "all") -> list[CheckResult]:
    if status == "issues":
        filtered = [r for r in results if r.status in ("WARN", "FAIL", "ERROR")]
    elif status == "all":
        filtered = list(results)
    else:
        filtered = [r for r in results if r.status == status.upper()]

    if severity != "all":
        filtered = [r for r in filtered if r.severity == severity]
    return filtered


def render_description(result: CheckResult, detail: str) -> list[str]:
    if detail == "compact":
        return []
    lines = []
    if result.description:
        lines.append(f"       {result.description}")
    if detail == "verbose":
        lines.append(f"       id={result.name} | severity={result.severity}")
    return lines


def render_report(
    results: list[CheckResult],
    catalog: str,
    *,
    detail: str = "normal",
    shown_count: int | None = None,
    total_count: int | None = None,
) -> str:
    icon = {"OK": "✅", "WARN": "⚠️ ", "FAIL": "❌", "ERROR": "💥"}
    lines = [f"DQ Suite -- catalog={catalog}", "=" * 70]
    name_width = max((len(result_label(r)) for r in results), default=20)
    for r in results:
        label = result_label(r)
        lines.append(f"{icon.get(r.status, '?')} {label:<{name_width}}  {r.detail}")
        lines.extend(render_description(r, detail))
    lines.append("")
    ok = sum(1 for r in results if r.status == "OK")
    warn = sum(1 for r in results if r.status == "WARN")
    fail = sum(1 for r in results if r.status == "FAIL")
    err = sum(1 for r in results if r.status == "ERROR")
    lines.append(f"✅ {ok} ok | ⚠️ {warn} warn | ❌ {fail} fail | 💥 {err} error")
    if shown_count is not None and total_count is not None and shown_count != total_count:
        lines.append(f"🔎 mostrando {shown_count} de {total_count} checks")
    return "\n".join(lines)


def markdown_rows(results: list[CheckResult], detail: str) -> list[str]:
    if detail == "compact":
        return [
            f"| `{markdown_cell(result_label(r))}` | {markdown_cell(r.detail)} |"
            for r in results
        ]
    if detail == "verbose":
        return [
            (
                f"| `{markdown_cell(result_label(r))}` | `{markdown_cell(r.name)}` | "
                f"{markdown_cell(r.severity)} | {markdown_cell(r.description)} | {markdown_cell(r.detail)} |"
            )
            for r in results
        ]
    return [
        f"| `{markdown_cell(result_label(r))}` | {markdown_cell(r.description)} | {markdown_cell(r.detail)} |"
        for r in results
    ]


def markdown_header(detail: str) -> list[str]:
    if detail == "compact":
        return ["| Check | Detail |", "|-------|--------|"]
    if detail == "verbose":
        return ["| Check | ID | Severity | Description | Detail |", "|-------|----|----------|-------------|--------|"]
    return ["| Check | Description | Detail |", "|-------|-------------|--------|"]


def render_markdown_report(results: list[CheckResult], catalog: str, *, detail: str = "normal") -> str:
    """Render results as GitHub-flavoured markdown (for PR comments / step summary)."""
    ok = [r for r in results if r.status == "OK"]
    warns = [r for r in results if r.status == "WARN"]
    fails = [r for r in results if r.status in ("FAIL", "ERROR")]

    overall = "✅ All checks passed" if not fails and not warns else ("❌ Failures detected" if fails else "⚠️ Warnings")
    lines = [
        f"## 🔍 Data Quality — `{catalog}`",
        f"> {overall}",
        "",
    ]

    if fails:
        lines += [
            "### ❌ Failures",
            *markdown_header(detail),
            *markdown_rows(fails, detail),
            "",
        ]

    if warns:
        lines += [
            "### ⚠️ Warnings",
            *markdown_header(detail),
            *markdown_rows(warns, detail),
            "",
        ]

    if ok:
        lines += [
            f"<details><summary>✅ {len(ok)} checks passed</summary>",
            "",
            *markdown_header(detail),
            *markdown_rows(ok, detail),
            "",
            "</details>",
            "",
        ]

    counts = f"✅ {len(ok)} ok"
    if warns:
        counts += f" | ⚠️ {len(warns)} warn"
    if fails:
        counts += f" | ❌ {len(fails)} fail"
    lines.append(f"**{counts}**")

    return "\n".join(lines)


def main() -> int:
    configure_output_encoding()

    ap = argparse.ArgumentParser(description="Roda a suite de DQ contra um catalog.")
    ap.add_argument("--target", choices=("dev", "prod"), default="dev")
    ap.add_argument("--catalog", help="Override do catalog. Sobrepõe --target.")
    ap.add_argument("--output", choices=("console", "markdown"), default="console",
                    help="Formato de saída. 'markdown' imprime para stdout (uso em CI).")
    ap.add_argument(
        "--status",
        choices=("all", "issues", "ok", "warn", "fail", "error"),
        default="all",
        help="Filtra o que aparece no relatorio. 'issues' mostra WARN/FAIL/ERROR.",
    )
    ap.add_argument(
        "--severity",
        choices=("all", "error", "warn"),
        default="all",
        help="Filtra pela severidade declarada do check.",
    )
    ap.add_argument(
        "--detail",
        choices=("compact", "normal", "verbose"),
        default="normal",
        help="Nivel de detalhe do relatorio.",
    )
    args = ap.parse_args()

    load_env(ENV_FILE)

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not host or not token or not warehouse_id:
        raise SystemExit("DATABRICKS_HOST, DATABRICKS_TOKEN e DATABRICKS_WAREHOUSE_ID precisam estar no .env")

    catalog = args.catalog or CATALOG_BY_TARGET[args.target]

    from .suite import SUITE

    client = DatabricksSQL(host=host, token=token, warehouse_id=warehouse_id)
    results = run_suite(client, catalog, SUITE)
    displayed = filter_results(results, status=args.status, severity=args.severity)

    if args.output == "markdown":
        print(render_markdown_report(displayed, catalog, detail=args.detail))
    else:
        print(
            render_report(
                displayed,
                catalog,
                detail=args.detail,
                shown_count=len(displayed),
                total_count=len(results),
            )
        )

    failed = any(r.status in ("FAIL", "ERROR") for r in results)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
