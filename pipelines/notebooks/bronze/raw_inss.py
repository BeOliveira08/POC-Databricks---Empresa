# Databricks notebook source
# bronze: inss_pdf

# COMMAND ----------
# MAGIC %run ../../config/project_params

# COMMAND ----------
# MAGIC %run ../../utils/readers

# COMMAND ----------
# MAGIC %run ../../utils/delta

# COMMAND ----------
# MAGIC %run ../../utils/pdf

# COMMAND ----------
# MAGIC %run ../../utils/text

# COMMAND ----------

import os
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

BRONZE_INSS_PDF = f"{CATALOG}.{BRONZE}.raw_inss"

CPF_PATTERN = re.compile(r"CPF:\s*([\d\.\-]+)", re.IGNORECASE)
DATA_NASCIMENTO_PATTERN = re.compile(r"Data de nascimento\s*:?\s*([0-3]?\d/[01]?\d/\d{4})", re.IGNORECASE)
INLINE_SEQ_PATTERN = re.compile(r"^(\d{1,3})\s+(.+)$")
INLINE_CNPJ_PATTERN = re.compile(r"^(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})(?:\s+(.*))?$")
TIPO_PATTERN = re.compile(
    r"^(.*?)\s+"
    r"(Empregado ou Agente P[úu]blico|Contribuinte Individual|Facultativo)"
    r"(?:\s+(\d{2}/\d{2}/\d{4}))?"
    r"(?:\s+(\d{2}/\d{2}/\d{4}))?"
    r"(?:\s+(\d{2}/\d{4}))?",
    re.IGNORECASE,
)


def is_noise_line(linha: str) -> bool:
    if not linha:
        return True

    ruido = {
        "INSS",
        "CNIS - Cadastro Nacional de Informações Sociais",
        "Extrato Previdenciário",
        "Identificação do Filiado",
        "Código Emp.",
        "Origem do Vínculo",
        "Tipo Filiado no",
        "Vínculo",
        "Data Início",
        "Data Fim",
        "Últ. Remun.",
        "Seq.",
        "NIT",
        "Matrícula do",
        "Trabalhador",
        "Competência",
        "Remunerações",
        "Relações Previdenciárias",
        "Valores Consolidados por Ano Civil",
        "Legenda de Indicadores",
    }

    if linha in ruido:
        return True

    for prefixo in [
        "Página ",
        "Data de nascimento",
        "Nome da mãe",
        "Nome:",
        "NIT:",
        "CPF:",
        "Você pode conferir a autenticidade",
        "com o código",
        "Indicador Descrição",
    ]:
        if linha.startswith(prefixo):
            return True

    return False


def parse_inss_pdf(path: str):
    texto = extract_pdf_text(path)
    arquivo = os.path.basename(path)
    cpf_match = CPF_PATTERN.search(texto)
    cpf_raw = clean_text(cpf_match.group(1)) if cpf_match else None
    dt_nascimento_match = DATA_NASCIMENTO_PATTERN.search(texto)
    dt_nascimento_raw = clean_text(dt_nascimento_match.group(1)) if dt_nascimento_match else None

    raw_linhas = [l.strip() for l in texto.split("\n") if l is not None]
    linhas = []
    for linha in raw_linhas:
        inline_seq = INLINE_SEQ_PATTERN.match(linha or "")
        if inline_seq and not re.fullmatch(r"\d{2}/\d{4}.*", linha or ""):
            linhas.append(inline_seq.group(1))
            linhas.append(inline_seq.group(2).strip())
        else:
            linhas.append(linha)

    def is_seq_line(s: str) -> bool:
        return bool(re.fullmatch(r"\d{1,3}", s or ""))

    def is_nit_line(s: str) -> bool:
        return bool(re.fullmatch(r"\d{3}\.\d{5}\.\d{2}-\d", s or ""))

    def is_data_dd(s: str) -> bool:
        return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", s or ""))

    def is_data_mm(s: str) -> bool:
        return bool(re.fullmatch(r"\d{2}/\d{4}", s or ""))

    def is_valor(s: str) -> bool:
        return bool(re.fullmatch(r"[\d\.]+,\d{2}", s or ""))

    blocos = []
    i = 0
    while i < len(linhas):
        if is_seq_line(linhas[i]):
            j = i + 1
            while j < len(linhas) and (not linhas[j] or is_noise_line(linhas[j])):
                j += 1

            if j < len(linhas) and linhas[j] and not is_noise_line(linhas[j]):
                ini = i
                k = j + 1
                while k < len(linhas):
                    atual = linhas[k]
                    if atual.startswith("Relações Previdenciárias") or atual.startswith("Valores Consolidados por Ano Civil"):
                        break
                    if is_seq_line(atual):
                        z = k + 1
                        while z < len(linhas) and (not linhas[z] or is_noise_line(linhas[z])):
                            z += 1
                        if z < len(linhas) and linhas[z] and not is_noise_line(linhas[z]):
                            break
                    k += 1
                blocos.append(linhas[ini:k])
                i = k
                continue
        i += 1

    rows = []
    for bloco in blocos:
        seq_raw = bloco[0] if bloco else None
        pos = 1
        while pos < len(bloco) and (not bloco[pos] or is_noise_line(bloco[pos])):
            pos += 1

        codigo_emp_raw = bloco[pos] if pos < len(bloco) else None
        nome_empresa = None
        cnpj_raw = None
        if codigo_emp_raw:
            inline_cnpj = INLINE_CNPJ_PATTERN.match(codigo_emp_raw)
            if inline_cnpj:
                cnpj_raw = inline_cnpj.group(1)
                nome_empresa = clean_text(inline_cnpj.group(2))
            pos += 1

        header_parts = []
        while pos < len(bloco):
            atual = bloco[pos]
            if not atual or is_noise_line(atual):
                pos += 1
                continue
            if atual in {"Competência", "Remunerações"} or is_data_mm(atual) or is_valor(atual):
                break
            header_parts.append(atual)
            pos += 1

        header_txt = re.sub(r"\s+", " ", " ".join(header_parts)).strip()
        tipo_filiado_raw = None
        dt_inicio_raw = None
        dt_fim_raw = None
        ult_remun_raw = None

        if not nome_empresa and header_txt:
            nome_empresa = header_txt

        header_match = TIPO_PATTERN.search(header_txt or "")
        if header_match:
            nome_empresa = clean_text(header_match.group(1)) or nome_empresa
            tipo_filiado_raw = clean_text(header_match.group(2))
            dt_inicio_raw = clean_text(header_match.group(3))
            dt_fim_raw = clean_text(header_match.group(4))
            ult_remun_raw = clean_text(header_match.group(5))

        nit_raw = None
        while pos < len(bloco):
            atual = bloco[pos]
            if atual in {"Competência", "Remunerações"}:
                pos += 1
                break
            if not atual or is_noise_line(atual):
                pos += 1
                continue
            if is_nit_line(atual) and nit_raw is None:
                nit_raw = atual
            elif is_data_dd(atual):
                if dt_inicio_raw is None:
                    dt_inicio_raw = atual
                elif dt_fim_raw is None:
                    dt_fim_raw = atual
            elif is_data_mm(atual) and ult_remun_raw is None:
                ult_remun_raw = atual
            pos += 1

        if dt_fim_raw is None and ult_remun_raw is not None:
            dt_fim_raw = ult_remun_raw

        while pos < len(bloco):
            atual = bloco[pos]
            if not atual or is_noise_line(atual):
                pos += 1
                continue
            if is_data_mm(atual):
                competencia_raw = atual
                prox = pos + 1
                while prox < len(bloco) and (not bloco[prox] or is_noise_line(bloco[prox])):
                    prox += 1
                if prox < len(bloco) and is_valor(bloco[prox]):
                    rows.append(
                        {
                            "cpf": cpf_raw,
                            "dt_nascimento": dt_nascimento_raw,
                            "seq": clean_text(seq_raw),
                            "empresa": nome_empresa,
                            "cnpj": cnpj_raw or clean_text(codigo_emp_raw),
                            "tipo_filiado": tipo_filiado_raw,
                            "nit": nit_raw,
                            "dt_inicio": dt_inicio_raw,
                            "dt_fim": dt_fim_raw,
                            "competencia": competencia_raw,
                            "vl_base_inss": clean_text(bloco[prox]),
                            "arquivo_origem": arquivo,
                        }
                    )
                    pos = prox + 1
                    continue
            pos += 1

    return rows


schema = StructType(
    [
        StructField("cpf", StringType(), True),
        StructField("dt_nascimento", StringType(), True),
        StructField("seq", StringType(), True),
        StructField("empresa", StringType(), True),
        StructField("cnpj", StringType(), True),
        StructField("tipo_filiado", StringType(), True),
        StructField("nit", StringType(), True),
        StructField("dt_inicio", StringType(), True),
        StructField("dt_fim", StringType(), True),
        StructField("competencia", StringType(), True),
        StructField("vl_base_inss", StringType(), True),
        StructField("arquivo_origem", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

pdfs = list_files_by_extension(INSS_DIR, [".pdf"])
rows = []
for pdf_path in pdfs:
    rows.extend(parse_inss_pdf(pdf_path))

if rows:
    df_bronze = spark.createDataFrame(rows, schema=schema).withColumn("ingested_at", F.current_timestamp())
else:
    df_bronze = spark.createDataFrame([], schema=schema)

write_overwrite(df_bronze, BRONZE_INSS_PDF, catalog_schema=f"{CATALOG}.{BRONZE}")
