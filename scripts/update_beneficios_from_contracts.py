from __future__ import annotations

import argparse
import csv
from pathlib import Path


GENERIC_BENEFITS = {
    "plano_saude": "Plano Premium",
    "plano_odontologico": "Plano Odonto",
    "wellhub": "Ativo",
    "lazer": "Clube",
    "day_off_aniversario": "Sim",
    "seguro_vida": "Cobertura Basica",
    "vr_va": "950",
    "Nome _va/vr": "Cartao Flex",
    "bem_estar": "Programa de apoio",
    "idiomas": "Opcional",
    "participacao_lucro": "Sim",
    "observacao": "Beneficios sinteticos para demonstracao de portfolio.",
    "fonte": "base_generica_portfolio",
}


def update_csv(input_path: Path, output_path: Path) -> None:
    with input_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    for field in GENERIC_BENEFITS:
        if field not in fieldnames:
            fieldnames.append(field)

    for row in rows:
        for field, value in GENERIC_BENEFITS.items():
            row[field] = row.get(field) or value

    with output_path.open("w", encoding="utf-8", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Atualiza uma base de beneficios com valores sinteticos.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_csv", type=Path)
    args = parser.parse_args()
    update_csv(args.input_csv, args.output_csv)


if __name__ == "__main__":
    main()
