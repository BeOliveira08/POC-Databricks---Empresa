# Databricks notebook source
# metadata_templates.py
#
# Reusable templates and helpers for Unity Catalog data governance.
# Defines standard columns, tags, and table templates by layer.

# ============================================================================
# STANDARD COLUMNS (reusable across tables)
# ============================================================================

AUDIT_COLUMNS = [
    {"name": "pipeline_run_id", "comment": "ID da execução do pipeline"},
    {"name": "ingested_at", "comment": "Timestamp de ingestão/atualização"},
]

PII_PESSOA = [
    {"name": "nu_cpf", "comment": "CPF normalizado (11 dígitos) [PII]"},
    {"name": "nome", "comment": "Nome completo em maiúsculas [PII]"},
]

PII_EMPRESA = [
    {"name": "nu_cnpj", "comment": "CNPJ normalizado (14 dígitos)"},
    {"name": "nome_empresa", "comment": "Razão social da empresa"},
]

ENTITY_KEYS = [
    {"name": "id_empresa", "comment": "ID único da empresa, priorizando o CNPJ quando válido"},
    {"name": "entity_key", "comment": "Chave de entidade normalizada"},
]

VINCULO_BASE = [
    {"name": "tipo_contrato", "comment": "Tipo de vínculo (CLT, PJ, etc)"},
    {"name": "dt_inicio", "comment": "Data de início do vínculo"},
    {"name": "dt_fim", "comment": "Data de fim do vínculo (null se ativo)"},
    {"name": "cargo", "comment": "Cargo/função"},
    {"name": "senioridade", "comment": "Nível de senioridade"},
]

FINANCEIRO_BASE = [
    {"name": "vl_remuneracao", "comment": "Valor de remuneração mensal (R$)"},
]

# ============================================================================
# LAYER TEMPLATES
# ============================================================================

def bronze_table(name, desc, format_type="delta"):
    """Template para tabelas Bronze (raw data, sem transformações).

    Args:
        name: Nome da tabela (sem schema)
        desc: Descrição da tabela
        format_type: Formato dos dados (delta, jsonl, pdf_extraction, etc)

    Returns:
        Dict com metadata da tabela Bronze
    """
    return {
        "table_name": f"{{catalog}}.bronze.{name}",
        "table_description": desc,
        "tags": {
            "layer": "bronze",
            "format": format_type,
            "environment": "{env}",
        },
        "columns": [],  # Bronze geralmente sem column-level metadata
    }


def silver_table(name, desc, source, columns):
    """Template para tabelas Silver (staging, cleaned, deduplicated).

    Args:
        name: Nome da tabela (sem schema)
        desc: Descrição da tabela
        source: Tabela bronze de origem (ex: portfolio.bronze.contratos_geral)
        columns: Lista de dicts com column metadata

    Returns:
        Dict com metadata da tabela Silver

    Note:
        AUDIT_COLUMNS são automaticamente adicionadas ao final.
    """
    return {
        "table_name": f"{{catalog}}.silver.{name}",
        "table_description": desc,
        "tags": {
            "layer": "silver",
            "source": source,
            "environment": "{env}",
        },
        "columns": columns + AUDIT_COLUMNS,
    }


def gold_table(name, desc, domain, sensitivity, pii, columns):
    """Template para tabelas Gold (analytical, aggregated, business-ready).

    Args:
        name: Nome da tabela (sem schema)
        desc: Descrição da tabela
        domain: Domínio de negócio (fiscal, rh, financeiro, etc)
        sensitivity: Nível de sensibilidade (low, medium, high)
        pii: Se contém dados pessoais identificáveis (True/False)
        columns: Lista de dicts com column metadata

    Returns:
        Dict com metadata da tabela Gold

    Note:
        AUDIT_COLUMNS são automaticamente adicionadas ao final.
    """
    return {
        "table_name": f"{{catalog}}.gold.{name}",
        "table_description": desc,
        "tags": {
            "layer": "gold",
            "domain": domain,
            "sensitivity": sensitivity,
            "pii": str(pii).lower(),
            "environment": "{env}",
        },
        "columns": columns + AUDIT_COLUMNS,
    }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def col(name, comment, **kwargs):
    """Helper para criar column metadata de forma concisa.

    Args:
        name: Nome da coluna
        comment: Descrição da coluna
        **kwargs: Atributos extras (type, nullable, etc) - para uso futuro

    Returns:
        Dict com metadata da coluna

    Example:
        col("cpf", "CPF do titular [PII]")
        col("valor_total", "Valor total da transação (R$)")
    """
    result = {"name": name, "comment": comment}
    result.update(kwargs)
    return result
