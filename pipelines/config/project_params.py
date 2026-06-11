# Databricks notebook source
# project_params

import uuid

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = "portfolio_dev"

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

try:
    VOL_BASE = dbutils.widgets.get("volume_path")
except Exception:
    VOL_BASE = "/Volumes/portfolio/default/arquivos"

CONTRATOS_DIR = f"{VOL_BASE}/contratos"
CONTRATOS_CLT_DIR = f"{CONTRATOS_DIR}/CLT"
CONTRATOS_PJ_DIR = f"{CONTRATOS_DIR}/PJ"
CONTRATOS_FILES_DIR = f"{VOL_BASE}/contratos_files"
PROPOSTAS_DIR = f"{VOL_BASE}/propostas"
DISTRATOS_DIR = f"{VOL_BASE}/distratos"
CTPS_DIR = f"{VOL_BASE}/ctps"
FGTS_DIR = f"{VOL_BASE}/fgts"
INSS_DIR = f"{VOL_BASE}/inss"
IRPF_DIR = f"{VOL_BASE}/irpf"
DAS_DIR = f"{VOL_BASE}/das"
FUNCIONARIOS_DIR = f"{VOL_BASE}/funcionarios"
REFERENCIA_DIR = f"{VOL_BASE}/referencia"
EXCLUSIVIDADE_DIR = f"{REFERENCIA_DIR}/exclusividade"
EXCLUSIVIDADE_CONTRATOS_FILE = f"{EXCLUSIVIDADE_DIR}/base_exclusividade_contratos.csv"
EMPRESAS_DIR = f"{REFERENCIA_DIR}/empresas"
EMPRESA_ALIASES_CONFIG_PATH = f"{EMPRESAS_DIR}/empresa_aliases.csv"
EMPRESAS_CLT_DIR = f"{EMPRESAS_DIR}/clt"
EMPRESAS_PJ_DIR = f"{EMPRESAS_DIR}/pj"
CLIENTES_DIR = f"{REFERENCIA_DIR}/clientes"
CLIENTES_BASE_FILE = f"{CLIENTES_DIR}/Base_Clientes.csv"
NOTAS_EMITIDAS_DIR = f"{REFERENCIA_DIR}/notas_emitidas"
BENEFICIOS_DIR = f"{REFERENCIA_DIR}/beneficios"
FUNCIONARIOS_REFERENCIA_DIR = f"{REFERENCIA_DIR}/funcionarios"
FUNCIONARIOS_BASE_FILE = f"{FUNCIONARIOS_REFERENCIA_DIR}/Base_Funcionarios.csv"
FUNCIONARIOS_EMPRESA_BASE_FILE = f"{FUNCIONARIOS_REFERENCIA_DIR}/Base_Funcionarios_Empresa.csv"
REUNIOES_CONTRATACAO_DIR = f"{REFERENCIA_DIR}/reunioes_contratacao"
RESCISOES_DIR = f"{VOL_BASE}/rescisoes"

QUARANTINE_PATH = f"{VOL_BASE}/quarantine"
STAGING_UPLOAD_DIR = f"{VOL_BASE}/_staging_upload"

PIPELINE_RUN_ID = str(uuid.uuid4())
