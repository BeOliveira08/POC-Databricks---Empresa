from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path


BASE_DIR = Path(r"D:\portfolio_lakehouse_generic\arquivos_sanitizados_20260609_112320")
CONTRATOS_DIR = BASE_DIR / "contratos"
CONTRATOS_FILES_DIR = BASE_DIR / "contratos_files"
DEPARA_PATH = CONTRATOS_DIR / "depara_doc.csv"
RAW_CONTRATOS_PATH = CONTRATOS_DIR / "raw_contratos.csv"


INTERNAL_COMPANY_PATTERN = re.compile(r"\b(PORTFOLIO|SJ\s*GENERICO|PROVIDER_A)\b", re.IGNORECASE)
PJ_NAME_PATTERN = re.compile(
    r"\b(PJ|PRESTACAO\s+DE\s+SERVICOS|PRESTAÇÃO\s+DE\s+SERVIÇOS|ORDEM\s+SERVICO|ORDEM\s+DE\s+SERVICO|DISTRATO)\b",
    re.IGNORECASE,
)
CLT_NAME_PATTERN = re.compile(
    r"\b(CTPS|TRABALHO|ADMISSIONAL|CARTA\s+PROPOSTA|CONTRATACAO|CONTRATAÇÃO|TELETRABALHO|FICHA\s+DE\s+REGISTRO)\b",
    re.IGNORECASE,
)


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return ascii_text.upper()


def load_raw_classification() -> dict[str, set[str]]:
    by_file: dict[str, set[str]] = {}
    with RAW_CONTRATOS_PATH.open("r", encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            arquivo = (row.get("arquivo_origem") or "").strip()
            tp_vinculo = (row.get("tp_vinculo") or "").strip().upper()
            if not arquivo:
                continue
            by_file.setdefault(arquivo, set()).add(tp_vinculo or "NAO_INFORMADO")
    return by_file


def classify_file_name(name: str) -> tuple[str, str]:
    normalized = normalize_text(name)
    if INTERNAL_COMPANY_PATTERN.search(normalized):
        return "PJ", "nome_arquivo_empresa_propria"
    if PJ_NAME_PATTERN.search(normalized):
        return "PJ", "nome_arquivo_pj"
    if CLT_NAME_PATTERN.search(normalized):
        return "CLT", "nome_arquivo_clt"
    return "REVISAR", "sem_sinal_no_nome"


def build_doc_id(index: int) -> str:
    return f"DOC_{index:04d}"


def main() -> None:
    raw_by_file = load_raw_classification()
    pdfs = sorted(CONTRATOS_FILES_DIR.glob("*.pdf"), key=lambda p: p.name.upper())

    rows: list[dict[str, str]] = []

    for idx, pdf in enumerate(pdfs, start=1):
        arquivo = pdf.name
        raw_types = raw_by_file.get(arquivo, set())

        if raw_types == {"CLT"}:
            tipo_certeza = "CLT"
            pasta_destino = "ctps"
            fonte_classificacao = "raw_contratos"
        elif raw_types == {"PJ"}:
            tipo_certeza = "PJ"
            pasta_destino = "contratos_pj"
            fonte_classificacao = "raw_contratos"
        elif raw_types:
            tipo_certeza = "REVISAR"
            pasta_destino = "revisar"
            fonte_classificacao = f"raw_contratos_misto:{','.join(sorted(raw_types))}"
        else:
            tipo_certeza, fonte_classificacao = classify_file_name(arquivo)
            pasta_destino = "ctps" if tipo_certeza == "CLT" else "contratos_pj" if tipo_certeza == "PJ" else "revisar"

        rows.append(
            {
                "ARQUIVO_ORIGEM": arquivo,
                "DOCUMENTO_BASE": arquivo,
                "DOC_ID": build_doc_id(idx),
                "TIPO_CERTEZA": tipo_certeza,
                "PASTA_DESTINO": pasta_destino,
                "FONTE_CLASSIFICACAO": fonte_classificacao,
                "RAW_TP_VINCULO": ",".join(sorted(raw_types)) if raw_types else "",
            }
        )

    with DEPARA_PATH.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ARQUIVO_ORIGEM",
                "DOCUMENTO_BASE",
                "DOC_ID",
                "TIPO_CERTEZA",
                "PASTA_DESTINO",
                "FONTE_CLASSIFICACAO",
                "RAW_TP_VINCULO",
            ],
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(rows)

    summary: dict[str, int] = {}
    for row in rows:
        summary[row["PASTA_DESTINO"]] = summary.get(row["PASTA_DESTINO"], 0) + 1

    print(f"Arquivo gerado: {DEPARA_PATH}")
    print(f"Total de arquivos: {len(rows)}")
    for pasta, qtd in sorted(summary.items()):
        print(f"{pasta}: {qtd}")


if __name__ == "__main__":
    main()
