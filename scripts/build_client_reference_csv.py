from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import defaultdict
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GERAL_PATH = REPO_ROOT / "tmp_ref_csvs" / "Base_Empresas.csv"
DEFAULT_CLT_PATH = REPO_ROOT / "tmp_ref_csvs" / "BASE_CLT.csv"
DEFAULT_PJ_PATH = REPO_ROOT / "tmp_ref_csvs" / "Base_PJ.csv"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "tmp_ref_csvs" / "clientes" / "Base_Clientes.csv"

OUTPUT_COLUMNS = [
    "CONSULTORIA",
    "EMPRESA_CLIENTE",
    "DT_INICIO",
    "DT_FIM",
    "TIPO_CONTRATO",
    "FONTES_ORIGEM",
    "OBSERVACAO",
]

EMPTY_VALUES = {"", "NULL", "N/A", "NA", "NAO INFORMADO", "-", "NONE"}


def normalize_ascii(value: str | None) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_spaces(value: str | None) -> str:
    return re.sub(r"\s+", " ", normalize_ascii(value)).strip()


def normalize_key(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", normalize_spaces(value).upper()).strip()


def clean_value(value: str | None) -> str:
    cleaned = normalize_spaces(value)
    return "" if normalize_key(cleaned) in EMPTY_VALUES else cleaned


def split_clients(value: str | None) -> list[str]:
    cleaned = clean_value(value)
    if not cleaned:
        return []
    parts = re.split(r"\s*(?:/|\|)\s*", cleaned)
    result: list[str] = []
    for part in parts:
        client = clean_value(part)
        if client and client not in result:
            result.append(client)
    return result


def parse_date(value: str | None) -> datetime.date | None:
    raw = (value or "").strip()
    if not raw or raw.lower() == "null":
        return None
    raw = raw.replace('"', "")
    month_map = {
        "jan.": "Jan",
        "fev.": "Feb",
        "mar.": "Mar",
        "abr.": "Apr",
        "mai.": "May",
        "jun.": "Jun",
        "jul.": "Jul",
        "ago.": "Aug",
        "set.": "Sep",
        "out.": "Oct",
        "nov.": "Nov",
        "dez.": "Dec",
    }
    raw_lower = raw.lower()
    for pt, en in month_map.items():
        if raw_lower.startswith(pt):
            raw = en + raw[len(pt):]
            break
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def fmt_date(value: datetime.date | None) -> str:
    return value.isoformat() if value else ""


def merge_contract_type(existing: str, new_value: str) -> str:
    tokens = []
    for raw in [existing, new_value]:
        for token in re.split(r"[^A-Za-z]+", normalize_ascii(raw).upper()):
            if token and token not in tokens:
                tokens.append(token)
    preferred = ["CLT", "PJ", "FLEX", "COOPERATIVA"]
    ordered = sorted(tokens, key=lambda t: (preferred.index(t) if t in preferred else len(preferred), t))
    return "\\".join(ordered)


def load_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def update_entry(store: dict, consultoria: str, cliente: str, dt_inicio, dt_fim, tipo: str, fonte: str, obs: str = "") -> None:
    key = (normalize_key(consultoria), normalize_key(cliente))
    if not key[1]:
        return
    current = store.get(key)
    if current is None:
        store[key] = {
            "CONSULTORIA": consultoria,
            "EMPRESA_CLIENTE": cliente,
            "DT_INICIO": dt_inicio,
            "DT_FIM": dt_fim,
            "TIPO_CONTRATO": tipo,
            "FONTES_ORIGEM": {fonte},
            "OBSERVACAO": obs,
        }
        return

    if consultoria and not current["CONSULTORIA"]:
        current["CONSULTORIA"] = consultoria
    if dt_inicio and (current["DT_INICIO"] is None or dt_inicio < current["DT_INICIO"]):
        current["DT_INICIO"] = dt_inicio
    if dt_fim and (current["DT_FIM"] is None or dt_fim > current["DT_FIM"]):
        current["DT_FIM"] = dt_fim
    if tipo:
        current["TIPO_CONTRATO"] = merge_contract_type(current["TIPO_CONTRATO"], tipo) if current["TIPO_CONTRATO"] else tipo
    current["FONTES_ORIGEM"].add(fonte)
    if obs and not current["OBSERVACAO"]:
        current["OBSERVACAO"] = obs


def process_geral(rows: list[dict[str, str]], store: dict) -> None:
    for row in rows:
        consultoria = clean_value(row.get("CONSULTORIA"))
        tipo = clean_value(row.get("TIPO_CONTRATO"))
        obs = clean_value(row.get("MOTIVO_SAIDA"))
        for cliente in split_clients(row.get("EMPRESA_CLIENTE")):
            update_entry(store, consultoria, cliente, None, None, tipo, "Base_Empresas.csv", obs)


def process_clt(rows: list[dict[str, str]], store: dict) -> None:
    for row in rows:
        consultoria = clean_value(row.get("Empresa"))
        tipo = clean_value(row.get("TIPO_CONTRATO")) or "CLT"
        dt_inicio = parse_date(row.get("Data Início") or row.get("Data Inicio"))
        dt_fim = parse_date(row.get("Data Fim"))
        for cliente in split_clients(row.get("EMPRESA_CLIENTE")):
            update_entry(store, consultoria, cliente, dt_inicio, dt_fim, tipo, "BASE_CLT.csv", "Datas provisórias herdadas da consultoria")


def process_pj(rows: list[dict[str, str]], store: dict) -> None:
    for row in rows:
        consultoria = clean_value(row.get("CONSULTORIA"))
        tipo = clean_value(row.get("TP_VINCULO")) or "PJ"
        dt_inicio = parse_date(row.get("DT_INICIO"))
        dt_fim = parse_date(row.get("DT_FIM"))
        for cliente in split_clients(row.get("EMPRESA_CLIENTE")):
            update_entry(store, consultoria, cliente, dt_inicio, dt_fim, tipo, "Base_PJ.csv", "Datas provisórias herdadas da consultoria")


def write_output(path: Path, store: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for item in sorted(store.values(), key=lambda r: (normalize_key(r["CONSULTORIA"]), normalize_key(r["EMPRESA_CLIENTE"]))):
        rows.append(
            {
                "CONSULTORIA": item["CONSULTORIA"],
                "EMPRESA_CLIENTE": item["EMPRESA_CLIENTE"],
                "DT_INICIO": fmt_date(item["DT_INICIO"]),
                "DT_FIM": fmt_date(item["DT_FIM"]),
                "TIPO_CONTRATO": item["TIPO_CONTRATO"],
                "FONTES_ORIGEM": " | ".join(sorted(item["FONTES_ORIGEM"])),
                "OBSERVACAO": item["OBSERVACAO"],
            }
        )
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera Base_Clientes.csv separada do cadastro geral de empresas.")
    parser.add_argument("--geral", type=Path, default=DEFAULT_GERAL_PATH)
    parser.add_argument("--clt", type=Path, default=DEFAULT_CLT_PATH)
    parser.add_argument("--pj", type=Path, default=DEFAULT_PJ_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--secondary-output",
        type=Path,
        default=None,
        help="Copia opcionalmente o mesmo CSV para um segundo destino, como a pasta de referencia do volume.",
    )
    args = parser.parse_args()

    store: dict[tuple[str, str], dict[str, object]] = {}
    process_geral(load_rows(args.geral), store)
    process_clt(load_rows(args.clt), store)
    process_pj(load_rows(args.pj), store)
    write_output(args.output, store)
    if args.secondary_output is not None:
        write_output(args.secondary_output, store)
    print(f"[OK] CSV de clientes gerado em: {args.output}")
    if args.secondary_output is not None:
        print(f"[OK] Copia secundaria gerada em: {args.secondary_output}")
    print(f"[OK] Total de pares consultoria/cliente: {len(store)}")


if __name__ == "__main__":
    main()
