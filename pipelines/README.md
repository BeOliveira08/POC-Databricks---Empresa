# Pipelines

Documentacao resumida da estrutura de notebooks e jobs deste template.

## Estrutura de pastas

```text
pipelines/
|-- notebooks/
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|   |-- _frozen/
|   `-- setup/
|-- config/
|-- utils/
`-- data_governance/
```

## Convencoes

1. `bronze` recebe ingestao e parsing bruto.
2. `silver` aplica limpeza, padronizacao e reconciliacao.
3. `gold` publica fatos, dimensoes e agregados prontos para analise.
4. `setup` cria catalogo, schemas, tabelas base e infraestrutura minima.

## Jobs ativos

Os jobs principais ficam em `resources/*.yml` e sao carregados pelo bundle definido em `databricks.yml`.

## Como evoluir o template

1. Adicione o notebook na camada correta.
2. Use `%run` de `project_params` no inicio.
3. Evite hardcode de catalogo, schema, volume ou host.
4. Registre novas tabelas em `pipelines/data_governance/catalog_metadata.py`.
