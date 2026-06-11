"""Tipos de check para a suite de DQ.

Adicionar novo tipo:
    1. Criar classe que herda de Check
    2. Implementar render_sql(catalog) e evaluate(count)
    3. Importar e usar em suite.py
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CheckResult:
    name: str
    table: str
    status: str  # OK | FAIL | WARN | ERROR
    violations: int
    detail: str = ""
    severity: str = "error"
    display_name: str = ""
    description: str = ""


class Check:
    severity: str = "error"  # error | warn
    name: str
    table: str
    display_name: str = ""
    description: str = ""

    def render_sql(self, catalog: str) -> str:
        raise NotImplementedError

    def evaluate(self, count: int) -> tuple[str, str]:
        raise NotImplementedError


@dataclass
class Expression(Check):
    """Conta linhas onde a rule é FALSE (ou seja, viola). 0 = ok.

    A rule pode referenciar outras tabelas via placeholder `{catalog}`,
    substituído em runtime. Ex:
        rule="EXISTS (SELECT 1 FROM {catalog}.gold.outra WHERE ...)"

    Use `table_alias` quando a rule precisar de um alias SQL na tabela principal:
        table="gold.fato_irpf_resumo", table_alias="r",
        rule="r.vl_total ... (SELECT ... FROM {catalog}.gold.outra)"
    """

    table: str
    rule: str
    name: str
    severity: str = "error"
    table_alias: str = ""
    display_name: str = ""
    description: str = ""

    def render_sql(self, catalog: str) -> str:
        rule = self.rule.replace("{catalog}", catalog)
        ref = f"{catalog}.{self.table}"
        if self.table_alias:
            ref = f"{ref} {self.table_alias}"
        return f"SELECT COUNT(*) FROM {ref} WHERE NOT ({rule})"

    def evaluate(self, count: int) -> tuple[str, str]:
        if count == 0:
            return ("OK", "0 violations")
        return ("FAIL" if self.severity == "error" else "WARN", f"{count} violations")


@dataclass
class NotNull(Check):
    """Conta linhas onde a coluna é NULL. 0 = ok."""

    table: str
    col: str
    severity: str = "error"
    display_name: str = ""
    description: str = ""

    @property
    def name(self) -> str:
        return f"{self.table}.{self.col} not null"

    def render_sql(self, catalog: str) -> str:
        return f"SELECT COUNT(*) FROM {catalog}.{self.table} WHERE {self.col} IS NULL"

    def evaluate(self, count: int) -> tuple[str, str]:
        if count == 0:
            return ("OK", "0 nulls")
        return ("FAIL" if self.severity == "error" else "WARN", f"{count} nulls")


@dataclass
class RefIntegrity(Check):
    """Conta FKs do fato que não existem na dim. 0 = ok."""

    fact: str
    fact_col: str
    dim: str
    dim_col: str
    name: str
    severity: str = "error"
    display_name: str = ""
    description: str = ""

    @property
    def table(self) -> str:
        return self.fact

    def render_sql(self, catalog: str) -> str:
        return (
            f"SELECT COUNT(*) FROM {catalog}.{self.fact} f "
            f"LEFT JOIN {catalog}.{self.dim} d ON f.{self.fact_col} = d.{self.dim_col} "
            f"WHERE f.{self.fact_col} IS NOT NULL AND d.{self.dim_col} IS NULL"
        )

    def evaluate(self, count: int) -> tuple[str, str]:
        if count == 0:
            return ("OK", "0 orphans")
        return ("FAIL" if self.severity == "error" else "WARN", f"{count} orphans")


@dataclass
class Unique(Check):
    """Conta duplicatas da coluna. 0 = unique."""

    table: str
    col: str
    severity: str = "error"
    display_name: str = ""
    description: str = ""

    @property
    def name(self) -> str:
        return f"{self.table}.{self.col} unique"

    def render_sql(self, catalog: str) -> str:
        return (
            f"SELECT COUNT(*) - COUNT(DISTINCT {self.col}) "
            f"FROM {catalog}.{self.table} WHERE {self.col} IS NOT NULL"
        )

    def evaluate(self, count: int) -> tuple[str, str]:
        if count == 0:
            return ("OK", "unique")
        return ("FAIL" if self.severity == "error" else "WARN", f"{count} duplicates")


@dataclass
class UniqueComposite(Check):
    """Conta grupos duplicados de uma chave composta. 0 = unique.

    Usa subquery GROUP BY + HAVING para não depender de CONCAT.
    NULLs em qualquer coluna da chave são excluídos da verificação.
    """

    table: str
    cols: list[str]
    severity: str = "error"
    display_name: str = ""
    description: str = ""

    @property
    def name(self) -> str:
        return f"{self.table}.({', '.join(self.cols)}) unique"

    def render_sql(self, catalog: str) -> str:
        cols_sql = ", ".join(self.cols)
        not_null = " AND ".join(f"{c} IS NOT NULL" for c in self.cols)
        return (
            f"SELECT COUNT(*) FROM ("
            f"SELECT {cols_sql} FROM {catalog}.{self.table} "
            f"WHERE {not_null} "
            f"GROUP BY {cols_sql} HAVING COUNT(*) > 1"
            f")"
        )

    def evaluate(self, count: int) -> tuple[str, str]:
        if count == 0:
            return ("OK", "unique")
        return ("FAIL" if self.severity == "error" else "WARN", f"{count} duplicate groups")


@dataclass
class RowCountMin(Check):
    """Garante mínimo de linhas. Útil pra detectar tabela vazia."""

    table: str
    min_rows: int = 1
    severity: str = "error"
    display_name: str = ""
    description: str = ""

    @property
    def name(self) -> str:
        return f"{self.table} >= {self.min_rows} row(s)"

    def render_sql(self, catalog: str) -> str:
        return f"SELECT COUNT(*) FROM {catalog}.{self.table}"

    def evaluate(self, rows: int) -> tuple[str, str]:
        if rows >= self.min_rows:
            return ("OK", f"{rows} rows")
        return ("FAIL" if self.severity == "error" else "WARN", f"only {rows} rows (min {self.min_rows})")