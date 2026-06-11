# Governanca de Dados

Metadados, tags e comentarios de colunas para o template de lakehouse.

`catalog_metadata.py` e a fonte central de verdade para descricoes de tabela, tags e cobertura minima de governanca.

## Fluxo sugerido

1. Crie ou ajuste a tabela em `bronze`, `silver` ou `gold`.
2. Atualize a entrada correspondente em `TABLE_METADATA`.
3. Rode `npm run governance:validate`.
4. Aplique os metadados no seu proprio workspace quando quiser.

## Tags recomendadas

- `domain`
- `layer`
- `sensitivity`
- `source_system`
- `environment`

## Observacao

Este repositrio foi neutralizado para portfolio. Ajuste descricoes, dominios e sensibilidade conforme o seu caso real.
