# Portfolio Lakehouse Template

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Platform](https://img.shields.io/badge/Platform-Databricks-red.svg)](https://databricks.com/)

Template de projeto pessoal para portfolio com arquitetura medallion no Databricks. O objetivo e mostrar organizacao de pipelines Bronze, Silver e Gold, governanca de dados, validacoes e publicacao via Databricks Asset Bundles, sem expor dados reais.

## O que este repositorio demonstra

- arquitetura medallion aplicada a um caso de processamento documental
- notebooks separados por camada e por dominio
- jobs e deploy empacotados com Databricks Asset Bundles
- validacoes de qualidade e governanca antes da promocao entre camadas
- estrutura sanitizada para demonstracao publica no GitHub

## O que foi removido desta copia

- referencias a pessoas e empresas reais
- hosts, catalogos, volumes e identificadores reais do ambiente original
- dados pessoais, arquivos auxiliares e exportacoes locais
- credenciais e qualquer configuracao presa a um workspace especifico

## Estrutura

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

## Fluxo resumido

1. `bronze`: leitura e padronizacao inicial das fontes.
2. `silver`: consolidacao, enriquecimento e regras de negocio.
3. `gold`: fatos, dimensoes e visoes analiticas.
4. `scripts/`: validacoes, exports e rotinas auxiliares.
5. `resources/`: definicao de jobs e targets do bundle.

## Configuracao local

1. Copie `.env.example` para `.env`.
2. Preencha `DATABRICKS_HOST`, `DATABRICKS_TOKEN` e `DATABRICKS_WAREHOUSE_ID`.
3. Ajuste `catalog_name` e `volume_path` em `databricks.yml` se quiser usar outros nomes.
4. Faça o upload dos arquivos de exemplo para o seu proprio volume ou adapte o caminho para uma massa de teste sua.

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

## Dados locais de exemplo

Se quiser manter o projeto 100% local antes do upload manual, use a copia sanitizada em:

`D:\portfolio_lakehouse_generic\arquivos_sanitizados_20260609_112320`

## Observacoes

- `databricks.yml` continua como template e precisa do host do seu workspace para deploy.
- Os notebooks estao organizados para demonstrar engenharia de dados e modelagem analitica, nao para expor um dataset especifico.
- Os nomes de algumas tabelas e regras ainda refletem o caso de uso original, mas o conteudo desta versao foi neutralizado para portfolio.

## Licenca

MIT. Veja [LICENSE](./LICENSE).
# POC-Databricks---Empresa
