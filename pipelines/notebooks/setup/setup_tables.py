# Databricks notebook source
# setup_tables - bronze schema rebuilt for the new PDF/CSV/XLSX base.

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------

BRONZE_RAW_EMPRESAS         = f"{CATALOG}.{BRONZE}.raw_empresas"
BRONZE_RAW_CLIENTES         = f"{CATALOG}.{BRONZE}.raw_clientes"
BRONZE_RAW_CTPS             = f"{CATALOG}.{BRONZE}.raw_ctps"
BRONZE_RAW_CONTRATOS        = f"{CATALOG}.{BRONZE}.raw_contratos"
BRONZE_RAW_DISTRATOS        = f"{CATALOG}.{BRONZE}.raw_distratos"
BRONZE_RAW_RESCISOES        = f"{CATALOG}.{BRONZE}.raw_rescisoes"
BRONZE_RAW_BENEFICIOS       = f"{CATALOG}.{BRONZE}.raw_beneficios"
BRONZE_RAW_BENEFICIOS_ITEM  = f"{CATALOG}.{BRONZE}.raw_beneficios_item"
BRONZE_RAW_FUNCIONARIOS     = f"{CATALOG}.{BRONZE}.raw_funcionarios"
BRONZE_RAW_FUNC_EMP         = f"{CATALOG}.{BRONZE}.raw_func_emp"
BRONZE_RAW_ENTREVISTAS      = f"{CATALOG}.{BRONZE}.raw_entrevistas"
BRONZE_RAW_INSS             = f"{CATALOG}.{BRONZE}.raw_inss"
BRONZE_RAW_FGTS             = f"{CATALOG}.{BRONZE}.raw_fgts"
BRONZE_RAW_PROPOSTAS        = f"{CATALOG}.{BRONZE}.raw_propostas"
BRONZE_RAW_DAS              = f"{CATALOG}.{BRONZE}.raw_das"
BRONZE_RAW_NF               = f"{CATALOG}.{BRONZE}.raw_nf"
BRONZE_RAW_IRPF             = f"{CATALOG}.{BRONZE}.raw_irpf"

LEGACY_BRONZE_TABLES = [
    f"{CATALOG}.{BRONZE}.ctps_vinculos",
    f"{CATALOG}.{BRONZE}.outros_classificados",
    f"{CATALOG}.{BRONZE}.inss_vinculos",
    f"{CATALOG}.{BRONZE}.contratos_geral",
    f"{CATALOG}.{BRONZE}.contratos_pj_cargo_depara",
    f"{CATALOG}.{BRONZE}.contratos_planilha",
    f"{CATALOG}.{BRONZE}.contratos_pdf",
    f"{CATALOG}.{BRONZE}.contratos_txt",
    f"{CATALOG}.{BRONZE}.contratos_unificado",
    f"{CATALOG}.{BRONZE}.distratos_pdf",
    f"{CATALOG}.{BRONZE}.rescisoes_pdf",
    f"{CATALOG}.{BRONZE}.ctps_planilha",
    f"{CATALOG}.{BRONZE}.ctps_pdf",
    f"{CATALOG}.{BRONZE}.propostas_pdf",
    f"{CATALOG}.{BRONZE}.nf_provider_a",
    f"{CATALOG}.{BRONZE}.nf_portfolio",
    f"{CATALOG}.{BRONZE}.empresas_planilha",
    f"{CATALOG}.{BRONZE}.beneficios_empresa",
    f"{CATALOG}.{BRONZE}.funcionarios_referencia",
    f"{CATALOG}.{BRONZE}.reunioes_contratacao",
    f"{CATALOG}.{BRONZE}.inss_pdf",
    f"{CATALOG}.{BRONZE}.fgts_extratos",
    f"{CATALOG}.{BRONZE}.das",
    f"{CATALOG}.{BRONZE}.irpf_declaracoes",
    f"{CATALOG}.{BRONZE}.funcionarios_levantamento",
    f"{CATALOG}.{BRONZE}.irpf_fontes_pagadoras",
    f"{CATALOG}.{BRONZE}.irpf_bens_direitos",
    f"{CATALOG}.{BRONZE}.contratos_pdf_texto",
]

ACTIVE_BRONZE_TABLES = [
    BRONZE_RAW_EMPRESAS,
    BRONZE_RAW_CLIENTES,
    BRONZE_RAW_CTPS,
    BRONZE_RAW_CONTRATOS,
    BRONZE_RAW_DISTRATOS,
    BRONZE_RAW_RESCISOES,
    BRONZE_RAW_BENEFICIOS,
    BRONZE_RAW_BENEFICIOS_ITEM,
    BRONZE_RAW_FUNCIONARIOS,
    BRONZE_RAW_FUNC_EMP,
    BRONZE_RAW_ENTREVISTAS,
    BRONZE_RAW_INSS,
    BRONZE_RAW_FGTS,
    BRONZE_RAW_PROPOSTAS,
    BRONZE_RAW_DAS,
    BRONZE_RAW_NF,
    BRONZE_RAW_IRPF,
]

# COMMAND ----------

for legacy_table in LEGACY_BRONZE_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {legacy_table}")
    print(f"[OK] legado removido: {legacy_table}")

for active_table in ACTIVE_BRONZE_TABLES:
    spark.sql(f"DROP TABLE IF EXISTS {active_table}")
    print(f"[OK] tabela reiniciada: {active_table}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_EMPRESAS} (
  consultoria_raw       STRING,
  empresa_cliente_raw   STRING,
  tipo_contrato_raw     STRING,
  motivo_saida_raw      STRING,
  arquivo_origem        STRING,
  ingested_at           TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_EMPRESAS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_CLIENTES} (
  consultoria_raw       STRING,
  empresa_cliente_raw   STRING,
  dt_inicio_raw         STRING,
  dt_fim_raw            STRING,
  tipo_contrato_raw     STRING,
  fontes_origem_raw     STRING,
  observacao_raw        STRING,
  arquivo_origem        STRING,
  ingested_at           TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_CLIENTES}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_CTPS} (
  fonte_referencia          STRING,
  tp_vinculo                STRING,
  nu_cpf                    STRING,
  nome                      STRING,
  nu_cnpj_empresa           STRING,
  nome_empresa              STRING,
  cargo                     STRING,
  cbo                       STRING,
  dt_inicio                 STRING,
  dt_fim                    STRING,
  vl_salario                STRING,
  status                    STRING,
  arquivo_origem_referencia STRING,
  arquivo_origem_txt        STRING,
  arquivo_pdf_validacao     STRING,
  fl_arquivo_pdf_existe     BOOLEAN,
  texto_bruto_documento     STRING,
  criterio_match_documento  STRING,
  ingested_at               TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_CTPS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_BENEFICIOS} (
  id_empresa_raw              STRING,
  cnpj_raw                    STRING,
  empresa_raw                 STRING,
  empresa_corrigida_raw       STRING,
  nome_operacional_raw        STRING,
  status_mapeamento_raw       STRING,
  observacao_mapeamento_raw   STRING,
  plano_saude_raw             STRING,
  plano_odontologico_raw      STRING,
  wellhub_raw                 STRING,
  lazer_raw                   STRING,
  day_off_aniversario_raw     STRING,
  seguro_vida_raw             STRING,
  vr_va_raw                   STRING,
  nome_vr_va_raw              STRING,
  bem_estar_raw               STRING,
  idiomas_raw                 STRING,
  participacao_lucro_raw      STRING,
  arquivo_origem              STRING,
  ingested_at                 TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_BENEFICIOS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_BENEFICIOS_ITEM} (
  id_empresa_raw              STRING,
  cnpj_raw                    STRING,
  empresa_raw                 STRING,
  empresa_corrigida_raw       STRING,
  nome_operacional_raw        STRING,
  status_mapeamento_raw       STRING,
  observacao_mapeamento_raw   STRING,
  tipo_beneficio_raw          STRING,
  grupo_beneficio_raw         STRING,
  campo_origem_raw            STRING,
  beneficio_nome_raw          STRING,
  beneficio_descricao_raw     STRING,
  beneficio_valor_raw         STRING,
  arquivo_origem              STRING,
  ingested_at                 TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_BENEFICIOS_ITEM}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_FUNCIONARIOS} (
  nome_funcionario_raw        STRING,
  telefone_raw                STRING,
  senioridade_raw             STRING,
  arquivo_origem              STRING,
  ingested_at                 TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_FUNCIONARIOS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_FUNC_EMP} (
  nome_funcionario_raw        STRING,
  empresa_funcionario_raw     STRING,
  status_raw                  STRING,
  arquivo_origem              STRING,
  ingested_at                 TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_FUNC_EMP}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_ENTREVISTAS} (
  event_id_raw                STRING,
  data_entrevista_raw         STRING,
  consultoria_raw             STRING,
  empresa_origem_raw          STRING,
  empresa_relacionada_raw     STRING,
  empresa_chave_raw           STRING,
  responsavel_raw             STRING,
  email_responsavel_raw       STRING,
  em_copia_raw                STRING,
  assunto_raw                 STRING,
  plataforma_raw              STRING,
  link_reuniao_raw            STRING,
  status_match_empresa_raw    STRING,
  arquivo_origem              STRING,
  ingested_at                 TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_ENTREVISTAS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_INSS} (
  cpf                  STRING,
  seq                  STRING,
  empresa              STRING,
  cnpj                 STRING,
  tipo_filiado         STRING,
  nit                  STRING,
  dt_inicio            STRING,
  dt_fim               STRING,
  competencia          STRING,
  vl_base_inss         STRING,
  arquivo_origem       STRING,
  ingested_at          TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_INSS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_FGTS} (
  nome               STRING,
  cpf                STRING,
  pis_pasep          STRING,
  empresa            STRING,
  cnpj               STRING,
  conta_fgts         STRING,
  dt_lancamento      STRING,
  descricao          STRING,
  valor              STRING,
  saldo              STRING,
  arquivo_origem     STRING,
  ingested_at        TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_FGTS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_CONTRATOS} (
  fonte_referencia             STRING,
  tp_vinculo                   STRING,
  empresa_prestador            STRING,
  cnpj_prestador               STRING,
  consultoria                  STRING,
  cnpj_consultoria             STRING,
  empresa_cliente              STRING,
  cargo                        STRING,
  dt_inicio                    STRING,
  dt_fim                       STRING,
  valor_mensal                 STRING,
  status                       STRING,
  arquivo_origem_referencia    STRING,
  arquivo_origem_txt           STRING,
  arquivo_pdf_validacao        STRING,
  fl_arquivo_pdf_existe        BOOLEAN,
  texto_bruto_contrato         STRING,
  tp_clausula_exclusividade    STRING,
  fl_clausula_exclusividade    INT,
  fl_clausula_nao_concorrencia INT,
  trecho_clausula_exclusividade STRING,
  criterio_extracao_clausula   STRING,
  score_extracao_clausula      INT,
  criterio_match_documento     STRING,
  ingested_at                  TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_CONTRATOS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_DISTRATOS} (
  empresa                    STRING,
  cnpj                       STRING,
  dt_inicio                  STRING,
  dt_fim                     STRING,
  motivo                     STRING,
  arquivo_origem_txt         STRING,
  arquivo_pdf_validacao      STRING,
  fl_arquivo_pdf_existe      BOOLEAN,
  texto_bruto_documento      STRING,
  criterio_origem            STRING,
  ingested_at                TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_DISTRATOS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_RESCISOES} (
  nome_profissional_raw      STRING,
  cpf_raw                    STRING,
  empresa_raw                STRING,
  cnpj_raw                   STRING,
  dt_rescisao_raw            STRING,
  tipo_rescisao_raw          STRING,
  arquivo_origem_txt         STRING,
  arquivo_pdf_validacao      STRING,
  fl_arquivo_pdf_existe      BOOLEAN,
  texto_bruto_documento      STRING,
  criterio_origem            STRING,
  ingested_at                TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_RESCISOES}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_PROPOSTAS} (
  empresa                STRING,
  cnpj                   STRING,
  cargo                  STRING,
  senioridade            STRING,
  dt_inicio_prevista     STRING,
  valor_proposto         STRING,
  arquivo_origem_txt     STRING,
  arquivo_pdf_validacao  STRING,
  fl_arquivo_pdf_existe  BOOLEAN,
  texto_bruto_documento  STRING,
  ingested_at            TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_PROPOSTAS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_DAS} (
  cnpj_prestador     STRING,
  razao_social       STRING,
  competencia        STRING,
  dt_vencimento      STRING,
  dt_arrecadacao     STRING,
  nr_documento       STRING,
  cd_tributo         STRING,
  ds_tributo         STRING,
  vl_principal       STRING,
  vl_multa           STRING,
  vl_juros           STRING,
  vl_total           STRING,
  arquivo_origem     STRING,
  ingested_at        TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_DAS}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_NF} (
  fonte                STRING,
  nf_numero            STRING,
  data_emissao         STRING,
  tomador_nome         STRING,
  tomador_cnpj         STRING,
  prestador_cnpj       STRING,
  valor_servicos       STRING,
  valor_deducoes       STRING,
  valor_iss            STRING,
  iss_retido           STRING,
  nf_paga              STRING,
  numero_guia          STRING,
  id_externo           STRING,
  arquivo_origem       STRING,
  ingested_at          TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_NF}")

# COMMAND ----------

spark.sql(f"""
CREATE TABLE {BRONZE_RAW_IRPF} (
  arquivo_origem      STRING,
  cpf                 STRING,
  nome                STRING,
  ano_exercicio       STRING,
  ano_calendario      STRING,
  tipo_documento      STRING,
  tipo_declaracao     STRING,
  ingested_at         TIMESTAMP
) USING DELTA
""")
print(f"[OK] {BRONZE_RAW_IRPF}")

print(f"\n[OK] setup bronze concluido - catalog={CATALOG} schema={BRONZE}")
