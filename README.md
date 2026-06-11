# POC Databricks - Empresa

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Databricks-red.svg)](https://databricks.com/)

Projeto de portfolio focado em engenharia de dados no Databricks, com arquitetura medallion, governanca, validacoes e organizacao de deploy via Databricks Asset Bundles.

O repositorio mostra como estruturar um pipeline fim a fim para ingestao, padronizacao, enriquecimento e consumo analitico de dados documentais e cadastrais, usando uma abordagem proxima do que se espera em um ambiente corporativo.

## Objetivo do projeto

Esta POC foi preparada para demonstrar conhecimento pratico em:

- arquitetura medallion com camadas Bronze, Silver e Gold
- organizacao de pipelines no Databricks
- modelagem analitica com fatos, dimensoes e tabelas de apoio
- governanca de dados e validacoes de consistencia
- empacotamento e deploy com Databricks Asset Bundles
- separacao entre configuracao, transformacoes, utilitarios e jobs

## O que este repositorio demonstra

- ingestao de multiplas fontes em notebooks organizados por camada
- tratamento progressivo dos dados da camada bruta ate a camada analitica
- pipeline de setup para catalogo, schemas e tabelas
- scripts auxiliares para validacao, exportacao e checagens locais
- estrutura pronta para publicacao no GitHub sem expor dados sensiveis

## Arquitetura

```text
Bronze
  -> leitura e padronizacao inicial das fontes

Silver
  -> limpeza, consolidacao, enriquecimento e regras de negocio

Gold
  -> fatos, dimensoes e visoes analiticas para consumo
```

## Estrutura do projeto

```text
pipelines/
  notebooks/
    bronze/
    silver/
    gold/
    setup/
  config/
  utils/
  data_governance/
resources/
scripts/
docs/
```

## Stack utilizada

- Databricks
- Python
- Databricks Asset Bundles
- Unity Catalog
- Git e GitHub

## Como executar localmente

1. Copie `.env.example` para `.env`.
2. Preencha `DATABRICKS_HOST`, `DATABRICKS_TOKEN` e `DATABRICKS_WAREHOUSE_ID`.
3. Ajuste `catalog_name` e `volume_path` em `databricks.yml` conforme o seu workspace.
4. Faça upload da sua massa de teste para o volume que sera usado no projeto.

## Comandos uteis

```bash
npm run validate
npm run bootstrap
npm run jobs:bronze
npm run jobs:silver
npm run jobs:gold
npm run governance:validate
databricks bundle validate
databricks bundle deploy -t dev
```

## Observacoes sobre a versao publicada

- Esta versao foi sanitizada para portfolio.
- Dados pessoais, identificadores reais, credenciais e referencias sensiveis foram removidos.
- A estrutura tecnica foi preservada para evidenciar a modelagem e a organizacao do pipeline.

## Possiveis proximos passos

- conectar o bundle a um workspace Databricks pessoal
- adicionar diagramas de arquitetura em `docs/`
- publicar evidencias de execucao e resultados anonimizados
- criar dashboards ou queries de demonstracao sobre a camada Gold

## Licenca

MIT. Veja [LICENSE](./LICENSE).
