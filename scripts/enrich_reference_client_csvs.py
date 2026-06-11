from __future__ import annotations

import argparse
import csv
from pathlib import Path


def enrich_rows(input_path: Path, output_path: Path) -> None:
    with input_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    extra_fields = [
        "cliente_canonico",
        "empresa_canonica",
        "status_enriquecimento",
        "observacao_enriquecimento",
    ]
    for field in extra_fields:
        if field not in fieldnames:
            fieldnames.append(field)

    for index, row in enumerate(rows, start=1):
        row["cliente_canonico"] = row.get("cliente_canonico") or f"CLIENTE_GENERICO_{index:03d}"
        row["empresa_canonica"] = row.get("empresa_canonica") or f"EMPRESA_GENERICA_{index:03d}"
        row["status_enriquecimento"] = "sintetico"
        row["observacao_enriquecimento"] = "Exemplo generico para portfolio"

    with output_path.open("w", encoding="utf-8", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Enriquece uma base CSV de referencia com valores genericos.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()
    enrich_rows(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
