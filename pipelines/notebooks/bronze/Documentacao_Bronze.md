# Documentacao_Bronze.md

```md
# Documentação da Camada Bronze

## Objetivo
A Bronze concentra os dados extraídos e padronizados das fontes documentais, fiscais e referenciais do projeto.

## Tabelas principais
- `portfolio.bronze.empresas`: cadastro mestre de empresas. Identificador oficial `id_empresa`; `entity_key` fica como chave técnica de merge.
- `portfolio.bronze.beneficios_empresa`: benefícios ofertados por empresa. Relacionamento por `id_empresa`.
- `portfolio.bronze.ctps_pdf`: vínculos extraídos da CTPS. Base do vínculo CLT.
- `portfolio.bronze.contratos_geral`: contratos formais. Complementa CTPS.
- `portfolio.bronze.inss_vinculos`: vínculo e remuneração por competência via INSS.
- `portfolio.bronze.fgts_extratos`: eventos mensais de FGTS.
- `portfolio.bronze.nf_provider_a`: notas da PROVIDER_A.
- `portfolio.bronze.nf_portfolio`: notas da PORTFOLIO.
- `portfolio.bronze.das` ou `portfolio.bronze.das_provider_a`: DAS por documento e tributo.
- `portfolio.bronze.irpf_declaracoes`: declarações de IRPF por exercício.

## Observações
- Campos `*_raw` devem ser preservados como texto.
- CNPJ, CPF, NIT e PIS devem ser tratados como string.
- A Bronze preserva o conteúdo bruto estruturado; não deve forçar grão analítico final.
```
