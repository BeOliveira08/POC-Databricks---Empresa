## Whatsapp Contratacao Draft

Objetivo:
- deixar pronta uma trilha generica para quando surgir uma base estruturada de WhatsApp
- manter fora da pipeline ativa ate existir fonte confiavel

Arquivos congelados:
- `bronze/raw_whatsapp_contatos.py`
- `silver/whatsapp_contatos.py`
- `gold/fato_whatsapp_contratacao.py`

Fonte esperada:
- `referencia/whatsapp_contatos/Base_Whatsapp_Contatos.csv`

Colunas minimas esperadas:
- `DATA_MENSAGEM`
- `TELEFONE`
- `NOME_CONTATO`
- `CONSULTORIA`
- `EMPRESA`
- `EMAIL`
- `MENSAGEM`

Comportamento planejado:
- bronze le CSV flexivel e projeta colunas raw
- silver normaliza telefone, data, empresa de contratacao e classifica evento textual
- gold cruza com `dim_empresa_contratacao` e tenta casar com `fato_reunioes_contratacao` em janela de 30 dias

Observacoes:
- nao esta plugado em `run_validated_silver.py`
- nao esta plugado em `run_validated_gold.py`
- nao tem governanca ativa porque ainda e apenas preparacao de modelo
