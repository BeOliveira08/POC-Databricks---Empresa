# Databricks notebook source
# run_validated_core
#
# Bootstrap bronze-only da remodelagem nova.
# Sobe apenas as tabelas bronze ativas e com fonte disponivel hoje.

# COMMAND ----------
print("[START] setup_catalog")

# COMMAND ----------
# MAGIC %run ./setup/setup_catalog

# COMMAND ----------
print("[START] setup_tables")

# COMMAND ----------
# MAGIC %run ./setup/setup_tables

# COMMAND ----------
print("[START] bronze.raw_empresas")

# COMMAND ----------
# MAGIC %run ./bronze/raw_empresas

# COMMAND ----------
print("[START] bronze.raw_clientes")

# COMMAND ----------
# MAGIC %run ./bronze/raw_clientes

# COMMAND ----------
print("[START] bronze.raw_ctps")

# COMMAND ----------
# MAGIC %run ./bronze/raw_ctps

# COMMAND ----------
print("[START] bronze.raw_inss")

# COMMAND ----------
# MAGIC %run ./bronze/raw_inss

# COMMAND ----------
print("[START] bronze.raw_fgts")

# COMMAND ----------
# MAGIC %run ./bronze/raw_fgts

# COMMAND ----------
print("[START] bronze.raw_das")

# COMMAND ----------
# MAGIC %run ./bronze/raw_das

# COMMAND ----------
print("[START] bronze.raw_nf")

# COMMAND ----------
# MAGIC %run ./bronze/raw_nf

# COMMAND ----------
print("[START] bronze.raw_irpf")

# COMMAND ----------
# MAGIC %run ./bronze/raw_irpf

# COMMAND ----------
print("[START] bronze.raw_contratos")

# COMMAND ----------
# MAGIC %run ./bronze/raw_contratos

# COMMAND ----------
print("[START] bronze.raw_distratos")

# COMMAND ----------
# MAGIC %run ./bronze/raw_distratos

# COMMAND ----------
print("[START] bronze.raw_rescisoes")

# COMMAND ----------
# MAGIC %run ./bronze/raw_rescisoes

# COMMAND ----------
print("[START] bronze.raw_propostas")

# COMMAND ----------
# MAGIC %run ./bronze/raw_propostas

# COMMAND ----------
print("[START] bronze.raw_beneficios")

# COMMAND ----------
# MAGIC %run ./bronze/raw_beneficios

# COMMAND ----------
print("[START] bronze.raw_funcionarios")

# COMMAND ----------
# MAGIC %run ./bronze/raw_funcionarios

# COMMAND ----------
print("[START] bronze.raw_func_emp")

# COMMAND ----------
# MAGIC %run ./bronze/raw_func_emp

# COMMAND ----------
print("[START] bronze.raw_entrevistas")

# COMMAND ----------
# MAGIC %run ./bronze/raw_entrevistas

# COMMAND ----------
print("[OK] Bootstrap bronze executado: raw_empresas + raw_clientes + raw_ctps + raw_inss + raw_fgts + raw_das + raw_nf + raw_irpf + raw_contratos + raw_distratos + raw_rescisoes + raw_propostas + raw_beneficios + raw_funcionarios + raw_func_emp + raw_entrevistas")
