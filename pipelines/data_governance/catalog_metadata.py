# Databricks notebook source
# catalog_metadata.py
#
# Single source of truth for active Portfolio Lakehouse Template governance metadata.

# COMMAND ----------
# MAGIC %run ./metadata_templates

# COMMAND ----------

import json

try:
    CATALOG = dbutils.widgets.get("catalog")
except Exception:
    CATALOG = globals().get("CATALOG", "portfolio")

try:
    ENV = dbutils.widgets.get("env")
except Exception:
    ENV = "prod" if CATALOG == "portfolio" else "dev"


def _resolve(template):
    """Resolve {catalog} and {env} placeholders in template."""
    s = json.dumps(template)
    s = s.replace("{catalog}", CATALOG).replace("{env}", ENV)
    return json.loads(s)


TABLE_METADATA = [
    # Gold core
    _resolve(gold_table(
        "fato_reunioes_contratacao",
        (
            "Fato analitico de reunioes relacionadas a processos de contratacao, "
            "entrevistas e alinhamentos consolidados a partir do historico de emails."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_reuniao_contratacao", "Surrogate key da reuniao de contratacao"),
            col("evento_ref_ts", "Timestamp de referencia do evento"),
            col("sk_data_reuniao", "FK para dim_data na data da reuniao"),
            col("sk_empresa_contratacao", "FK para dim_empresa_contratacao"),
            col("processo_seletivo_key", "Identificador estavel do processo seletivo associado ao evento"),
            col("tipo_evento", "Classificacao do evento consolidado"),
            col("plataforma", "Plataforma da reuniao"),
            col("status_reuniao", "Status derivado da reuniao"),
            col("empresa_match_origem", "Campo usado como origem principal do match"),
            col("confianca_match_empresa", "Confianca derivada do match de empresa"),
            col("status_mapeamento_final", "Status final do mapeamento de empresa"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_resultado_contratacao",
        (
            "Fato analitico de desfecho de processos seletivos, consolidando por "
            "processo e empresa de contratacao se a pessoa candidata passou, nao passou ou segue em andamento."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_resultado_contratacao", "Surrogate key do resultado consolidado do processo"),
            col("processo_seletivo_key", "Identificador estavel do processo seletivo"),
            col("sk_empresa_contratacao", "FK para dim_empresa_contratacao"),
            col("empresa_id", "ID tecnico da empresa final quando houver match"),
            col("sk_data_primeiro_evento", "FK para dim_data no primeiro evento do processo"),
            col("sk_data_ultimo_evento", "FK para dim_data no ultimo evento do processo"),
            col("qt_eventos_processo", "Quantidade de eventos consolidados no processo"),
            col("fl_passou", "Flag indicando que o processo resultou em aprovacao"),
            col("fl_nao_passou", "Flag indicando que o processo resultou em nao aprovacao"),
            col("fl_em_andamento", "Flag indicando processo ainda sem desfecho fechado"),
            col("status_resultado", "Status textual consolidado do processo"),
            col("criterio_resultado", "Criterio usado para inferir o resultado"),
            col("dt_primeiro_vinculo_match", "Primeira data de vinculo usada para confirmar aprovacao quando houver"),
        ]
    )),

    _resolve(gold_table(
        "fato_reunioes_contratacao_mensal",
        (
            "Resumo mensal de reunioes de contratacao para analise de volume, "
            "cancelamentos e distribuicao por tipo de evento."
        ),
        domain="rh",
        sensitivity="low",
        pii=False,
        columns=[
            col("sk_data_mes", "FK para dim_data no primeiro dia do mes"),
            col("qt_eventos", "Quantidade total de eventos consolidados no mes"),
            col("qt_eventos_com_empresa_match", "Quantidade de eventos do mes com empresa mapeada para a dimensao"),
            col("qt_eventos_com_processo", "Quantidade de eventos do mes com chave de processo seletivo preenchida"),
            col("pc_eventos_com_empresa_match", "Percentual de eventos do mes com empresa mapeada"),
            col("qt_empresas_distintas", "Quantidade de empresas distintas no mes"),
            col("qt_plataformas_distintas", "Quantidade de plataformas distintas no mes"),
        ]
    )),

    _resolve(gold_table(
        "fato_funcionarios",
        (
            "Fato operacional enxuto de funcionarios vinculados a empresas, "
            "publicando apenas o identificador interno, a empresa e a pessoa resolvida."
        ),
        domain="rh",
        sensitivity="medium",
        pii=True,
        columns=[
            col("id_funcionario_interno", "ID unico do registro de funcionario interno"),
            col("empresa_id", "ID da empresa associada"),
            col("sk_funcionario", "FK para dim_pessoa do funcionario levantado"),
            col("pipeline_run_id", "ID da execucao da pipeline gold"),
            col("ingested_at", "Timestamp de gravacao do registro gold"),
        ]
    )),

    _resolve(gold_table(
        "dim_empresa",
        (
            "Dimensao gold enxuta de empresas, publicando apenas o cadastro consolidado "
            "e os periodos observados na silver."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("empresa_id", "ID tecnico sequencial da empresa"),
            col("nu_cnpj", "CNPJ normalizado da empresa"),
            col("nm_empresa", "Nome consolidado de exibicao da empresa"),
            col("tp_empresa", "Tipo principal consolidado da empresa"),
            col("tp_empresa_papeis", "Papeis consolidados da empresa nas fontes de origem"),
            col("fl_tem_entrevista", "Flag indicando se a empresa apareceu em registros de entrevista ou contratacao"),
            col("fl_empresa_apenas_entrevista", "Flag indicando se a empresa so apareceu em entrevistas sem outras evidencias operacionais"),
            col("dt_primeira_evidencia", "Primeira data em que a empresa apareceu nas evidencias"),
            col("dt_ultima_evidencia", "Ultima data em que a empresa apareceu nas evidencias"),
            col("data_inicio", "Data inicial do periodo observado da empresa"),
            col("data_fim", "Data final do periodo observado da empresa quando valida"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "dim_cliente",
        (
            "Dimensao gold de clientes consolidada a partir da referencia consultoria-cliente, "
            "separada da dimensao de empresas para preservar historico de alocacao."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("cliente_id", "ID tecnico sequencial do cliente"),
            col("nm_cliente", "Nome consolidado do cliente"),
            col("fl_periodo_provisorio", "Flag indicando periodo ainda provisoriamente herdado da consultoria"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_empresa_contrato_resumo",
        (
            "Fato resumo por empresa com evidencias contratuais observadas, "
            "mantendo sinais de cliente, cargo, senioridade e exclusividade fora da dimensao cadastral."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("empresa_id", "ID tecnico da empresa associada"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa"),
            col("nome_empresa", "Nome canonico da empresa"),
            col("clientes_observados", "Clientes observados em contratos relacionados"),
            col("cargos_observados", "Cargos observados em contratos relacionados"),
            col("senioridades_observadas", "Senioridades observadas em contratos relacionados"),
            col("fl_tem_contrato_exclusivo", "Flag indicando evidencia de contrato exclusivo"),
            col("fl_tem_contrato_sem_exclusividade", "Flag indicando evidencia de contrato sem exclusividade"),
            col("fl_tem_clausula_nao_concorrencia", "Flag indicando evidencia de clausula de nao concorrencia"),
            col("ds_exclusividade_resumo", "Resumo textual dos tipos de exclusividade encontrados"),
            col("fl_tem_evidencia_contrato", "Flag indicando se ha alguma evidencia contratual para a empresa"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_empresa_mensal",
        (
            "Fato mensal de permanencia por empresa e tipo de contrato, "
            "base para contar empresas ativas por periodo e comparar CLT vs PJ."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_empresa_mes", "Surrogate key do fato mensal de empresa"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("sk_tipo_contrato", "FK para dim_tipo_contrato"),
            col("sk_data_mes", "FK para dim_data no primeiro dia do mes"),
            col("ano", "Ano de referencia"),
            col("mes", "Mes de referencia"),
            col("qt_vinculos_ativos", "Quantidade de vinculos ativos no mes"),
            col("qt_pessoas_ativas", "Quantidade de pessoas ativas no mes"),
            col("qt_dias_ativos_total", "Soma dos dias ativos observados no mes"),
            col("fl_empresa_ativa_mes", "Flag indicando empresa ativa no mes"),
            col("vl_remuneracao_mes", "Valor total de remuneracao proporcional no mes"),
            col("vl_valor_total_mensal", "Valor total mensal consolidado considerando remuneracao e faturamento quando aplicavel"),
            col("vl_remuneracao_media_mes", "Valor medio de remuneracao dos vinculos no mes"),
            col("vl_faturamento_mes", "Valor faturado no mes via notas fiscais quando aplicavel"),
            col("qt_notas_fiscais_mes", "Quantidade de notas fiscais associadas ao mes"),
        ]
    )),

    _resolve(gold_table(
        "fato_empresa_indicadores",
        (
            "Fato resumido por empresa e tipo de contrato com indicadores de "
            "permanencia, atividade e faturamento para analise executiva."
        ),
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_empresa_indicador", "Surrogate key do resumo de empresa"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("sk_tipo_contrato", "FK para dim_tipo_contrato"),
            col("sk_data_primeiro_inicio", "FK para dim_data na primeira data de inicio observada"),
            col("sk_data_ultimo_fim", "FK para dim_data na ultima data de fim observada"),
            col("sk_data_mes_pico", "FK para dim_data no mes de pico de pessoas ativas"),
            col("qt_vinculos", "Quantidade total de vinculos observados"),
            col("qt_pessoas_distintas", "Quantidade total de pessoas distintas"),
            col("qt_meses_ativos", "Quantidade de meses ativos observados"),
            col("qt_dias_permanencia_total", "Quantidade total de dias de permanencia somados"),
            col("qt_dias_permanencia_media", "Media de dias de permanencia por vinculo"),
            col("qt_meses_permanencia_media", "Media de meses de permanencia por vinculo"),
            col("qt_pessoas_ativas_media", "Media mensal de pessoas ativas"),
            col("qt_pessoas_pico", "Maior quantidade de pessoas ativas em um unico mes"),
            col("qt_vinculos_pico", "Maior quantidade de vinculos ativos em um unico mes"),
            col("fl_empresa_ativa_atual", "Flag indicando empresa ainda ativa no periodo corrente"),
            col("vl_remuneracao_media_contrato", "Remuneracao media observada no nivel de contrato"),
            col("vl_faturamento_total", "Faturamento total observado via notas fiscais"),
            col("vl_faturamento_medio_mensal", "Faturamento medio mensal observado"),
            col("vl_faturamento_mes_pico", "Faturamento observado no mes de pico de pessoas"),
        ]
    )),

    _resolve(gold_table(
        "dim_data",
        "Dimensao calendario para analises por dia, semana, mes, trimestre, semestre e ano.",
        domain="geral",
        sensitivity="low",
        pii=False,
        columns=[
            col("id_data", "ID tecnico da data no formato YYYYMMDD"),
            col("id_mes", "ID tecnico do mes no formato YYYYMM"),
            col("dt_referencia", "Data de referencia"),
            col("nr_dia_mes", "Dia do mes"),
            col("nr_dia_semana", "Dia da semana numerico"),
            col("ds_dia_semana", "Nome do dia da semana"),
            col("ds_dia_semana_abrev", "Nome abreviado do dia da semana"),
            col("fl_fim_de_semana", "Flag indicando fim de semana"),
            col("nr_semana_ano", "Numero da semana no ano"),
            col("nr_mes", "Numero do mes"),
            col("ds_mes", "Nome do mes"),
            col("ds_mes_abrev", "Nome abreviado do mes"),
            col("nr_trimestre", "Numero do trimestre"),
            col("ds_trimestre", "Descricao do trimestre"),
            col("nr_semestre", "Numero do semestre"),
            col("ds_semestre", "Descricao do semestre"),
            col("nr_ano", "Ano de referencia"),
        ]
    )),

    _resolve(gold_table(
        "dim_funcionario",
        (
            "Dimensao gold de funcionarios internos, derivada exclusivamente do "
            "cadastro silver de funcionarios."
        ),
        domain="rh",
        sensitivity="medium",
        pii=True,
        columns=[
            col("id_funcionario", "ID tecnico do funcionario"),
            col("nome_funcionario", "Nome consolidado do funcionario [PII]"),
            col("telefone", "Telefone consolidado do funcionario [PII]"),
            col("senioridade", "Senioridade consolidada do funcionario"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "dim_pessoa",
        (
            "Dimensao gold unificada de pessoas, preservando funcionarios internos "
            "e titulares documentais identificados por CPF para consumo fiscal e previdenciario."
        ),
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("id_pessoa", "ID da pessoa na dimensao; para registros DOCUMENTAL representa o titular mapeado por CPF"),
            col("nome_pessoa", "Nome consolidado da pessoa quando houver [PII]"),
            col("tipo_pessoa", "Origem principal da pessoa: FUNCIONARIO ou DOCUMENTAL"),
            col("pipeline_run_id_origem", "ID da execucao de origem do registro base"),
            col("ingested_at_origem", "Timestamp de ingestao do registro base"),
        ]
    )),

    _resolve(gold_table(
        "dim_cargo",
        "Dimensao de cargos e senioridades observados nos vinculos pessoa-empresa.",
        domain="rh",
        sensitivity="low",
        pii=False,
        columns=[
            col("id_cargo", "ID tecnico do cargo"),
            col("cargo", "Nome do cargo consolidado"),
            col("senioridade", "Melhor senioridade disponivel para o cargo"),
        ]
    )),

    _resolve(gold_table(
        "dim_tipo_contrato",
        "Dimensao de tipos de contrato e tipo de pessoa observados nos vinculos.",
        domain="rh",
        sensitivity="low",
        pii=False,
        columns=[
            col("id_tipo_contrato", "ID tecnico do tipo de contrato"),
            col("tipo_contrato", "Tipo de contrato consolidado"),
            col("tipo_pessoa", "Tipo de pessoa associado ao contrato"),
        ]
    )),

    _resolve(gold_table(
        "dim_empresa_contratacao",
        "Dimensao de empresas e consultorias observadas no funil de contratacao, mesmo quando nao existem na dim_empresa principal.",
        domain="rh",
        sensitivity="low",
        pii=False,
        columns=[
            col("id_empresa_contratacao", "ID tecnico da empresa de contratacao"),
            col("nm_empresa_contratacao", "Nome consolidado da empresa ou consultoria de contratacao"),
            col("tp_empresa_contratacao", "Tipo consolidado da entidade de contratacao"),
            col("empresa_id", "ID tecnico da dim_empresa quando houve match"),
            col("nm_empresa_match", "Nome da empresa da dim_empresa usada no match quando houver"),
            col("nm_consultoria_match", "Consultoria sugerida no arquivo de matches"),
            col("nm_consultoria_origem", "Consultoria original observada na base de reunioes"),
            col("nm_empresa_cliente_match", "Empresa cliente sugerida no arquivo de matches"),
            col("nm_empresa_portfolio_mapeada", "Empresa PORTFOLIO sugerida no arquivo de matches"),
            col("status_match_empresa", "Status final do match observado nas reunioes"),
            col("empresa_contratacao_key", "Chave tecnica normalizada da empresa de contratacao"),
        ]
    )),

    _resolve(gold_table(
        "dim_reuniao_contratacao_atributo",
        "Dimensao de atributos textuais e classificatorios de reunioes de contratacao.",
        domain="rh",
        sensitivity="low",
        pii=False,
        columns=[
            col("sk_reuniao_atributo", "Surrogate key dos atributos da reuniao"),
            col("tipo_evento", "Tipo do evento de contratacao"),
            col("plataforma", "Plataforma da reuniao"),
            col("status_reuniao", "Status derivado da reuniao"),
            col("empresa_match_origem", "Campo usado como origem principal do match"),
            col("confianca_match_empresa", "Confianca derivada do match de empresa"),
            col("status_mapeamento_final", "Status final do mapeamento de empresa"),
        ]
    )),

    _resolve(gold_table(
        "dim_funcionario_operacao_atributo",
        "Dimensao de atributos textuais do levantamento operacional de funcionarios.",
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_funcionario_operacao_atributo", "Surrogate key dos atributos operacionais"),
            col("consultoria", "Consultoria normalizada associada ao registro"),
            col("cliente", "Cliente normalizado associado ao registro"),
            col("tipo_contratacao", "Tipo de contratacao informado"),
            col("motivo_saida", "Motivo de saida informado"),
            col("status_operacao", "Status operacional derivado do levantamento"),
        ]
    )),

    _resolve(gold_table(
        "fato_vinculo_empresa",
        (
            "Fato gold de vinculos pessoa-empresa do core atual. No estado atual, "
            "o periodo do vinculo usa a janela de atividade observada da empresa; "
            "datas especificas do vinculo funcionario-empresa podem ser incorporadas depois."
        ),
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_vinculo", "Surrogate key do vinculo"),
            col("sk_pessoa", "FK para dim_pessoa do funcionario associado ao vinculo"),
            col("empresa_id", "ID tecnico da empresa"),
            col("consultoria_empresa_id", "ID tecnico da consultoria quando o vinculo estiver em contexto de alocacao consultoria-cliente"),
            col("cliente_contexto_id", "Identificador consolidado do cliente, preservando match em dimensao ou referencia operacional"),
            col("dt_inicio_cliente", "Data inicial do contexto de cliente quando houver mapeamento"),
            col("dt_fim_cliente", "Data final do contexto de cliente quando houver mapeamento"),
            col("fonte_cliente_contexto", "Origem usada para associar cliente ao vinculo, como referencia ou contrato"),
            col("sk_cargo", "FK para dim_cargo"),
            col("sk_tipo_contrato", "FK para dim_tipo_contrato"),
            col("sk_data_inicio", "FK para dim_data na data inicial atualmente herdada da atividade da empresa"),
            col("dt_inicio", "Data inicial atualmente herdada da atividade observada da empresa"),
            col("sk_data_fim", "FK para dim_data na data final atualmente herdada da atividade da empresa"),
            col("dt_fim", "Data final atualmente herdada da atividade observada da empresa"),
            col("vl_remuneracao", "Valor de remuneracao mensal do vinculo"),
            col("fl_exclusividade_contrato", "Flag de contrato exclusivo"),
            col("fl_nao_concorrencia", "Flag de clausula de nao concorrencia"),
            col("tp_clausula_exclusividade", "Classificacao compacta da clausula de exclusividade encontrada"),
            col("trecho_clausula_exclusividade", "Trecho enxuto da clausula encontrada no contrato"),
            col("criterio_extracao_clausula", "Criterio usado para classificar a clausula de exclusividade"),
            col("score_extracao_clausula", "Score heuristico da evidencia da clausula"),
            col("doc_id", "Documento principal associado ao vinculo"),
            col("fonte_vinculo", "Origem consolidada do vinculo no estado atual"),
            col("fl_validacao_minima_vinculo", "Flag indicando se o vinculo passou na validacao minima para subir a fato principal"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "bridge_vinculo_mes",
        (
            "Ponte mensal de vinculos ativos por pessoa e empresa, usada para "
            "salario medio ponderado, empresas ativas e duracao mensal."
        ),
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_vinculo_mes", "Surrogate key do vinculo no mes"),
            col("sk_vinculo", "FK para fato_vinculo_empresa"),
            col("sk_pessoa", "FK para dim_pessoa"),
            col("empresa_id", "ID tecnico da empresa"),
            col("sk_cargo", "FK para dim_cargo"),
            col("sk_tipo_contrato", "FK para dim_tipo_contrato"),
            col("sk_data_mes", "FK para dim_data no primeiro dia do mes"),
            col("ano", "Ano de referencia"),
            col("mes", "Mes de referencia"),
            col("sk_data_inicio", "FK para dim_data na data de inicio"),
            col("sk_data_fim", "FK para dim_data na data final"),
            col("dias_mes", "Quantidade de dias do mes de referencia"),
            col("dias_ativos_mes", "Quantidade de dias ativos do vinculo no mes"),
            col("fl_ativo_mes", "Flag indicando vinculo ativo no mes"),
            col("vl_remuneracao", "Valor de remuneracao mensal do vinculo"),
            col("vl_remuneracao_dia_ponderada", "Numerador para salario medio ponderado por dias ativos"),
            col("vl_remuneracao_mes_proporcional", "Valor proporcional da remuneracao no mes"),
            col("pipeline_run_id", "ID da execucao da pipeline gold"),
            col("ingested_at", "Timestamp de gravacao do registro gold"),
        ]
    )),

    _resolve(gold_table(
        "fato_fgts",
        (
            "Fato mensal de FGTS do core atual, agregando eventos por CPF, empresa "
            "e competencia com saldo final conhecido no mes."
        ),
        domain="previdencia",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_fgts", "Surrogate key do agregado mensal de FGTS"),
            col("sk_pessoa", "FK para dim_pessoa"),
            col("empresa_id", "ID tecnico da empresa quando houver match"),
            col("sk_data_competencia", "FK para dim_data na competencia"),
            col("valor_fgts", "Soma dos valores de eventos do FGTS no mes"),
            col("sk_data_vencimento", "FK para dim_data na data esperada de vencimento do deposito"),
            col("sk_data_primeiro_deposito", "FK para dim_data na data do primeiro deposito identificado"),
            col("valor_depositado", "Valor depositado identificado no mes"),
            col("valor_rendimento", "Valor de rendimento identificado no mes"),
            col("valor_saque", "Valor de saque identificado no mes"),
            col("valor_fgts_esperado", "Valor esperado de FGTS com base em 8% da remuneracao CLT proporcional"),
            col("valor_gap_recolhimento", "Gap de recolhimento entre esperado e depositado"),
            col("saldo_fgts", "Saldo final conhecido do FGTS no mes"),
            col("qt_eventos", "Quantidade de eventos agregados no mes"),
        ]
    )),

    _resolve(gold_table(
        "fato_inss",
        (
            "Fato de competencias de INSS do core atual, preservando o registro "
            "consolidado por CPF, empresa e competencia."
        ),
        domain="previdencia",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_inss", "Surrogate key do registro de INSS"),
            col("sk_pessoa", "FK para dim_pessoa"),
            col("dt_nascimento", "Data de nascimento do titular [PII]"),
            col("empresa_id", "ID tecnico da empresa quando houver match"),
            col("seq", "Sequencial do vinculo na origem"),
            col("sk_data_inicio", "FK para dim_data na data de inicio"),
            col("sk_data_fim", "FK para dim_data na data final"),
            col("sk_data_competencia", "FK para dim_data na competencia"),
            col("vl_remuneracao", "Valor de remuneracao da competencia"),
            col("vl_contribuicao_inss", "Valor da contribuicao de INSS da competencia"),
            col("pc_aliquota_efetiva_inss", "Aliquota efetiva de INSS sobre a remuneracao da competencia"),
            col("vl_salario_liquido_apos_inss", "Valor estimado liquido apos desconto de INSS"),
            col("qt_meses_contribuidos_acumulado", "Quantidade acumulada de meses de contribuicao ate a competencia"),
            col("vl_contribuicao_inss_acumulada", "Valor acumulado de contribuicao ate a competencia"),
            col("doc_id", "Documento de origem principal do registro"),
        ]
    )),

    _resolve({
        "table_name": "{catalog}.gold.vw_resumo_previdenciario_atual",
        "table_description": (
            "View gold de grao atual por pessoa com resumo previdenciario "
            "corrente e projecao simplificada de aposentadoria."
        ),
        "tags": {
            "layer": "gold",
            "domain": "previdencia",
            "sensitivity": "high",
            "pii": "true",
            "environment": "{env}",
        },
        "columns": [
            col("id_pessoa", "ID da pessoa documental usada no resumo previdenciario atual"),
            col("primeira_competencia", "Primeira competencia observada"),
            col("ultima_competencia", "Ultima competencia observada"),
            col("dt_nascimento", "Data de nascimento do titular [PII]"),
            col("idade_atual_anos", "Idade atual estimada em anos"),
            col("qt_meses_contribuidos", "Quantidade total de meses contribuidos"),
            col("qt_meses_faltantes_contribuicao", "Quantidade de meses faltantes para a regra simplificada de contribuicao"),
            col("qt_meses_faltantes_idade", "Quantidade de meses faltantes para a regra simplificada de idade"),
            col("qt_meses_faltantes_aposentadoria", "Quantidade de meses faltantes pela regra simplificada consolidada"),
            col("dt_aposentadoria_estimada", "Data estimada de aposentadoria pela regra simplificada"),
            col("vl_contribuicao_total", "Valor total de contribuicoes observadas"),
            col("vl_contribuicao_media_mensal", "Valor medio mensal de contribuicao"),
            col("vl_remuneracao_media_total", "Remuneracao media total observada"),
            col("vl_remuneracao_media_12m", "Remuneracao media das ultimas 12 competencias"),
            col("pc_fator_beneficio_estimado", "Fator percentual simplificado usado para estimar o beneficio"),
            col("vl_beneficio_estimado_mensal", "Valor mensal estimado de beneficio"),
        ],
    }),

    _resolve(gold_table(
        "fato_das",
        (
            "Fato de guias DAS do core atual, mantendo uma linha por documento "
            "e competencia com enriquecimento de empresa quando houver."
        ),
        domain="fiscal",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_das", "Surrogate key da guia DAS"),
            col("empresa_id", "ID tecnico da empresa quando houver match"),
            col("sk_data_competencia", "FK para dim_data na competencia"),
            col("competencia", "Competencia mensal da guia"),
            col("nr_documento", "Numero do documento da guia"),
            col("sk_data_vencimento", "FK para dim_data na data de vencimento"),
            col("dt_vencimento", "Data de vencimento da guia"),
            col("sk_data_arrecadacao", "FK para dim_data na data de arrecadacao"),
            col("dt_arrecadacao", "Data de arrecadacao da guia"),
            col("vl_principal", "Valor principal da guia"),
            col("vl_multa", "Valor de multa da guia"),
            col("vl_juros", "Valor de juros da guia"),
            col("vl_total", "Valor total da guia"),
        ]
    )),

    _resolve(gold_table(
        "fato_das_semestral",
        (
            "Fato agregado semestral de DAS, consolidando guias por empresa e "
            "semestre de competencia."
        ),
        domain="fiscal",
        sensitivity="medium",
        pii=False,
        columns=[
            col("sk_das_semestral", "Surrogate key da agregacao semestral de DAS"),
            col("empresa_id", "ID tecnico da empresa"),
            col("sk_data_semestre", "FK para dim_data no primeiro dia do semestre de referencia"),
            col("ano_ref", "Ano de referencia do semestre"),
            col("semestre_ref", "Numero do semestre de referencia"),
            col("qt_guias_das", "Quantidade de guias DAS consolidadas no semestre"),
            col("vl_principal_total", "Soma do valor principal das guias do semestre"),
            col("vl_multa_total", "Soma das multas das guias do semestre"),
            col("vl_juros_total", "Soma dos juros das guias do semestre"),
            col("vl_total_das", "Soma do valor total das guias do semestre"),
        ]
    )),

    _resolve(gold_table(
        "fato_notas_fiscais",
        (
            "Fato de notas fiscais emitidas por PORTFOLIO/PROVIDER_A, com status de pagamento "
            "para analise de recebiveis e clientes com notas em aberto."
        ),
        domain="financeiro",
        sensitivity="medium",
        pii=False,
        columns=[
            col("id_nota_fiscal", "ID tecnico da nota fiscal na silver"),
            col("nf_numero", "Numero da NFS-e"),
            col("id_externo", "Identificador externo da nota fiscal quando houver"),
            col("sk_data_competencia", "FK para dim_data na competencia"),
            col("sk_data_emissao", "FK para dim_data na data de emissao"),
            col("prestador_empresa_id", "ID tecnico da empresa prestadora"),
            col("tomador_empresa_id", "ID tecnico da empresa tomadora quando houver match"),
            col("valor_servicos", "Valor bruto dos servicos"),
            col("valor_deducoes", "Valor de deducoes da nota"),
            col("valor_iss", "Valor de ISS"),
            col("valor_liquido", "Valor liquido apos deducoes"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_irpf",
        (
            "Fato analitico de declaracoes de IRPF do core atual, com uma linha "
            "por CPF, exercicio e ano-calendario apos deduplicacao da silver."
        ),
        domain="fiscal",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_irpf", "Surrogate key da declaracao de IRPF"),
            col("id_irpf", "ID tecnico da declaracao na silver"),
            col("sk_pessoa", "FK para dim_pessoa"),
            col("exercicio", "Exercicio fiscal da declaracao"),
            col("ano_calendario", "Ano-calendario de referencia"),
            col("imposto_pago_total", "Valor total de imposto pago ou retido"),
            col("imposto_restituir", "Valor total a restituir"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_informe_rendimentos",
        "Fato analitico de informes de rendimentos por CPF, exercicio e ano-calendario.",
        domain="fiscal",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_informe_rendimentos", "Surrogate key do informe de rendimentos"),
            col("id_informe_rendimentos", "ID tecnico do informe na silver"),
            col("sk_pessoa", "FK para dim_pessoa; neste fato normalmente identifica a pessoa documental mapeada por CPF"),
            col("exercicio", "Exercicio fiscal do informe"),
            col("ano_calendario", "Ano-calendario de referencia"),
            col("rendimentos_tributaveis", "Valor de rendimentos tributaveis quando extraido"),
            col("imposto_retido_total", "Valor total de imposto retido ou pago"),
            col("imposto_restituir", "Valor total a restituir quando informado"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_imposto_serv",
        (
            "Fato tributario de ISS derivado das notas fiscais, separado da fato "
            "de recebiveis para facilitar analise de base, aliquota e retencao."
        ),
        domain="fiscal",
        sensitivity="medium",
        pii=False,
        columns=[
            col("id_nota_fiscal", "ID tecnico da nota fiscal de origem"),
            col("nf_numero", "Numero da NFS-e"),
            col("id_externo", "Identificador externo da nota quando informado"),
            col("prestador_empresa_id", "ID tecnico da empresa prestadora"),
            col("tomador_empresa_id", "ID tecnico da empresa tomadora quando houver match"),
            col("sk_data_competencia", "FK para dim_data na competencia"),
            col("sk_data_emissao", "FK para dim_data na data de emissao"),
            col("valor_base_iss", "Base tributavel estimada para calculo de ISS"),
            col("valor_iss", "Valor de ISS destacado"),
            col("pc_iss_sobre_base", "Percentual de ISS sobre a base tributavel estimada"),
            col("fl_iss_retido", "Flag indicando ISS retido na fonte"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "dim_rescisao",
        "Dimensao de eventos rescisorios CLT consolidados por empresa, CPF e data de rescisao.",
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("id_rescisao", "ID tecnico do evento rescisorio"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("cpf", "CPF do titular documental [PII]"),
            col("cnpj", "CNPJ da empresa rescisora"),
            col("nome_profissional", "Nome do profissional [PII]"),
            col("nome_empresa", "Nome da empresa"),
            col("tipo_rescisao", "Classificacao consolidada da rescisao"),
            col("dt_rescisao", "Data de rescisao consolidada"),
            col("qt_documentos_origem", "Quantidade de documentos consolidados no evento"),
            col("fl_tem_documento_pagamento", "Flag indicando existencia de comprovante de pagamento"),
            col("fl_tem_multa_rescisoria", "Flag indicando existencia de documento de multa rescisoria"),
            col("pipeline_run_id_origem", "ID da execucao silver que originou o evento"),
            col("ingested_at_origem", "Timestamp de ingestao de origem do evento"),
        ]
    )),

    _resolve(gold_table(
        "fato_rescisao_valores",
        "Fato de valores recebidos em rescisao CLT consolidado no grao do evento rescisorio.",
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_rescisao", "FK para dim_rescisao"),
            col("sk_pessoa", "FK para dim_pessoa; pessoa documental do titular"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("sk_data_rescisao", "FK para dim_data na data da rescisao"),
            col("vl_rescisao_bruta", "Valor bruto identificado na documentacao da rescisao"),
            col("vl_rescisao_liquida", "Valor liquido identificado na documentacao da rescisao"),
            col("vl_multa_rescisoria", "Valor da multa rescisoria identificado em documento especifico"),
            col("vl_total_recebido_calculado", "Valor total calculado da rescisao, combinando liquido e multa quando houver"),
            col("qt_documentos_origem", "Quantidade de documentos consolidados no evento"),
            col("pipeline_run_id_origem", "ID da execucao silver que originou o valor"),
            col("ingested_at_origem", "Timestamp de ingestao silver de origem"),
        ]
    )),

    _resolve(gold_table(
        "fato_rescisoes",
        "Fato analitico de documentos de rescisao extraidos de PDFs, amarrados a pessoa e data quando possivel.",
        domain="rh",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_rescisao", "Surrogate key da rescisao"),
            col("id_rescisao", "ID tecnico da rescisao na silver"),
            col("sk_pessoa", "FK para dim_pessoa; neste fato normalmente identifica a pessoa documental mapeada por CPF"),
            col("sk_data_rescisao", "FK para dim_data na data identificada no documento"),
            col("nome_empresa", "Nome da empresa extraido do documento"),
            col("tipo_rescisao", "Classificacao textual da rescisao"),
            col("dt_rescisao", "Data de referencia da rescisao"),
            col("file_name", "Nome do arquivo de origem"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_ctps",
        (
            "Fato de vinculos CTPS com periodo, salario e melhor evidencia contratual "
            "casada para clausulas de exclusividade e nao concorrencia."
        ),
        domain="rh",
        sensitivity="high",
        pii=False,
        columns=[
            col("sk_ctps", "Surrogate key do registro CTPS consolidado"),
            col("id_ctps", "ID tecnico do registro de CTPS na silver"),
            col("empresa_id", "ID tecnico da empresa empregadora quando identificada"),
            col("sk_cargo", "FK para dim_cargo"),
            col("sk_tipo_contrato", "FK para dim_tipo_contrato, esperado CLT"),
            col("sk_data_inicio", "FK para dim_data na data de inicio"),
            col("dt_inicio", "Data de inicio do vinculo"),
            col("sk_data_fim", "FK para dim_data na data de fim"),
            col("dt_fim", "Data de fim do vinculo"),
            col("doc_id", "Documento principal associado ao registro de CTPS quando houver"),
            col("vl_salario", "Salario observado na CTPS"),
            col("status_ctps", "Status consolidado do registro CTPS"),
            col("fl_exclusividade_contrato", "Flag indicando contrato com clausula de exclusividade"),
            col("fl_nao_concorrencia", "Flag indicando contrato com restricao de nao concorrencia"),
            col("tp_clausula_exclusividade", "Classificacao compacta da clausula encontrada"),
            col("trecho_clausula_exclusividade", "Trecho textual da melhor clausula encontrada"),
            col("criterio_extracao_clausula", "Estrategia usada pelo parser para classificar a clausula"),
            col("score_extracao_clausula", "Score heuristico da evidencia de clausula"),
            col("arquivo_origem_ctps", "Arquivo de origem do registro CTPS"),
            col("arquivo_origem_contrato", "Arquivo contratual casado para evidenciar a clausula"),
            col("criterio_match_contrato", "Criterio usado para casar CTPS com contrato"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "fato_beneficios_empresa",
        "Fato analitico de beneficios por empresa com indicadores derivados na camada gold.",
        domain="rh",
        sensitivity="medium",
        pii=False,
        columns=[
            col("id_beneficio", "ID tecnico do beneficio na silver"),
            col("empresa_id", "ID tecnico da empresa"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa"),
            col("nome_empresa", "Nome preferencial da empresa"),
            col("plano_saude", "Texto original de plano de saude"),
            col("plano_odontologico", "Texto original de plano odontologico"),
            col("wellhub", "Texto original de Wellhub/Gympass"),
            col("lazer", "Texto original de lazer"),
            col("day_off_aniversario", "Texto original de day off de aniversario"),
            col("seguro_vida", "Texto original de seguro de vida"),
            col("nome_va_vr", "Nome do beneficio de VR/VA quando informado"),
            col("vr_va_valor", "Valor monetario de VR/VA quando informado"),
            col("vr_va_descricao", "Descricao textual de VR/VA quando informada"),
            col("vr_va_status", "Status textual de VR/VA quando informado"),
            col("bem_estar", "Texto original de beneficio de bem-estar"),
            col("idiomas", "Texto original de beneficio de idiomas"),
            col("plr_valor", "Valor monetario de PLR quando informado"),
            col("fl_tem_vr_va", "Flag indicando beneficio VR/VA identificado"),
            col("fl_tem_plr", "Flag indicando beneficio PLR identificado"),
            col("fl_plano_saude", "Flag indicando plano de saude identificado"),
            col("fl_plano_odontologico", "Flag indicando plano odontologico identificado"),
            col("fl_wellhub", "Flag indicando Wellhub/Gympass identificado"),
            col("fl_lazer", "Flag indicando lazer identificado"),
            col("fl_day_off_aniversario", "Flag indicando day off de aniversario identificado"),
            col("fl_seguro_vida", "Flag indicando seguro de vida identificado"),
            col("fl_bem_estar", "Flag indicando beneficio de bem-estar identificado"),
            col("fl_idiomas", "Flag indicando beneficio de idiomas identificado"),
            col("pipeline_run_id_origem", "ID da execucao da silver que originou o registro"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na silver"),
        ]
    )),

    _resolve(gold_table(
        "alerta_completude_documental",
        (
            "Tabela gold de alertas operacionais e de qualidade para pendencias documentais, "
            "validacao minima de vinculos e reconciliacao anual de valores esperados vs observados."
        ),
        domain="governanca",
        sensitivity="high",
        pii=True,
        columns=[
            col("sk_alerta", "Surrogate key do alerta"),
            col("dominio", "Dominio funcional do alerta: VINCULO, FGTS, INSS, BENEFICIOS ou NOTAS_FISCAIS"),
            col("tabela_afetada", "Tabela gold diretamente impactada pelo alerta"),
            col("granularidade_alerta", "Granularidade do alerta: VINCULO, EMPRESA ou ANO"),
            col("nivel_alerta", "Severidade do alerta"),
            col("fl_bloqueia_consumo", "Flag indicando se a pendencia deve bloquear consumo confiavel do dado"),
            col("status_validacao", "Status consolidado da validacao"),
            col("item_pendente", "Item objetivo faltante ou inconsistente"),
            col("motivo_alerta", "Descricao resumida do motivo do alerta"),
            col("funcionario_empresa_id", "ID tecnico do vinculo associado quando houver"),
            col("id_pessoa", "ID tecnico da pessoa associada quando houver"),
            col("nu_cpf_cnpj", "Documento normalizado da pessoa associada quando houver [PII]"),
            col("empresa_id", "ID tecnico da empresa associada quando houver"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa associada quando houver"),
            col("nome_empresa", "Nome canonico da empresa associada quando houver"),
            col("tipo_pessoa", "Tipo de pessoa relacionado ao alerta quando houver"),
            col("tipo_contrato", "Tipo de contrato relacionado ao alerta quando houver"),
            col("dt_inicio_ref", "Data inicial de referencia do vinculo relacionado ao alerta"),
            col("dt_fim_ref", "Data final de referencia do vinculo relacionado ao alerta"),
            col("ano_ref", "Ano de referencia usado em reconciliacoes anuais"),
            col("doc_id", "Documento principal relacionado ao alerta quando houver"),
            col("fonte_vinculo", "Origem consolidada do vinculo quando houver"),
            col("nivel_confianca_vinculo", "Nivel de confianca do vinculo quando houver"),
            col("vl_valor_esperado", "Valor esperado para reconciliacao anual quando aplicavel"),
            col("vl_valor_observado", "Valor observado na fato comparada quando aplicavel"),
            col("vl_gap_valor", "Diferenca absoluta entre esperado e observado"),
            col("pc_gap_valor", "Diferenca percentual entre esperado e observado"),
            col("chave_alerta_legivel", "Chave textual legivel para explicar a unicidade do alerta"),
            col("pipeline_run_id", "ID da execucao da pipeline gold"),
            col("ingested_at", "Timestamp de gravacao do alerta"),
        ]
    )),

    # Silver active
    _resolve(silver_table(
        "tb_reunioes_contratacao",
        (
            "Tabela silver com reunioes de contratacao normalizadas, deduplicadas e "
            "enriquecidas com empresa PORTFOLIO quando identificada."
        ),
        source=f"{CATALOG}.bronze.reunioes_contratacao",
        columns=[
            col("id_reuniao_contratacao", "ID tecnico da reuniao de contratacao"),
            col("evento_ref_ts", "Timestamp de referencia do evento consolidado"),
            col("primeira_msg_ts", "Timestamp do primeiro email agrupado no evento"),
            col("ultima_msg_ts", "Timestamp do ultimo email agrupado no evento"),
            col("dt_reuniao", "Data da reuniao"),
            col("ano", "Ano da reuniao"),
            col("mes", "Mes da reuniao"),
            col("ano_mes", "Mes de referencia no formato YYYY-MM"),
            col("tipo_evento", "Classificacao do evento consolidado"),
            col("empresa_origem", "Empresa informada na origem"),
            col("empresa_relacionada", "Empresa relacionada informada na origem"),
            col("assunto_normalizado", "Assunto normalizado do evento"),
            col("plataforma", "Plataforma da reuniao"),
            col("qt_emails_evento", "Quantidade de emails agrupados no evento"),
            col("qt_notificacoes", "Quantidade de notificacoes agrupadas no evento"),
            col("qt_cancelados", "Quantidade de mensagens de cancelamento agrupadas"),
            col("tem_onboarding", "Flag de onboarding derivada do evento"),
            col("tem_treinamento", "Flag de treinamento derivada do evento"),
            col("status_reuniao", "Status derivado da reuniao"),
            col("assunto_processo", "Assunto tratado para agrupamento de processos seletivos"),
            col("processo_seletivo_key", "Identificador estavel do processo seletivo"),
            col("ordem_evento_processo", "Sequencia do evento dentro do processo seletivo"),
            col("qt_eventos_processo", "Quantidade total de eventos associados ao mesmo processo"),
            col("status_mapeamento_final", "Status final do mapeamento de empresa"),
            col("empresa_id", "ID da empresa associada quando houver match"),
            col("nu_cnpj_empresa", "CNPJ da empresa associada"),
            col("nome_empresa", "Nome canonico da empresa associada"),
            col("observacao", "Observacoes do levantamento"),
            col("pipeline_run_id_origem", "ID da execucao que produziu o registro na bronze"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na bronze"),
        ]
    )),

    _resolve(silver_table(
        "tb_funcionario",
        "Cadastro silver unico de funcionarios baseado na planilha de levantamento, usando o responsavel como nome principal.",
        source=f"{CATALOG}.bronze.raw_funcionarios",
        columns=[
            col("id_funcionario", "ID tecnico do funcionario"),
            col("nome_funcionario", "Nome do responsavel consolidado [PII]"),
            col("telefone", "Telefone do funcionario quando informado [PII]"),
            col("senioridade", "Senioridade consolidada do funcionario"),
        ]
    )),

    _resolve(silver_table(
        "tb_empresa",
        (
            "Tabela silver canonica de empresas, mantendo um cadastro padrao das empresas "
            "trabalhadas sem atributos de relacionamento ou chaves logicas antigas."
        ),
        source=f"{CATALOG}.bronze.empresas",
        columns=[
            col("empresa_id", "ID tecnico sequencial da empresa"),
            col("nu_cnpj", "CNPJ normalizado da empresa"),
            col("nm_empresa", "Nome preferencial de exibicao da empresa"),
            col("fl_tem_entrevista", "Flag indicando se a empresa apareceu em entrevistas de contratacao"),
            col("fl_empresa_apenas_entrevista", "Flag indicando se a empresa so apareceu em entrevistas e nao tem contrato ou historico de trabalho"),
            col("data_inicio", "Primeira data observada da empresa no historico"),
            col("data_fim", "Ultima data observada da empresa no historico quando houver"),
            col("pipeline_run_id_origem", "ID da execucao que produziu o registro na bronze"),
            col("ingested_at_origem", "Timestamp de ingestao do registro na bronze"),
        ]
    )),

    _resolve(silver_table(
        "tb_func_emp",
        (
            "Tabela silver de relacionamento funcionario-empresa, com um registro por vinculo "
            "observado entre funcionario e empresa."
        ),
        source=f"{CATALOG}.bronze.raw_func_emp",
        columns=[
            col("id_funcionario_empresa", "ID tecnico do relacionamento funcionario-empresa"),
            col("id_funcionario", "ID tecnico do funcionario"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("nome_funcionario", "Nome consolidado do funcionario [PII]"),
            col("nome_empresa", "Nome consolidado da empresa"),
            col("status", "Status informado do relacionamento"),
            col("pipeline_run_id", "ID tecnico da execucao da silver"),
        ]
    )),

    _resolve(silver_table(
        "tb_empresa_contratacao",
        "Tabela silver de relacionamento consolidado entre funcionario e empresa para contratacao, com inicio e fim por empresa.",
        source=f"{CATALOG}.silver.tb_func_emp|{CATALOG}.bronze.raw_funcionarios",
        columns=[
            col("id_empresa_contratacao", "ID tecnico do relacionamento consolidado de contratacao"),
            col("id_funcionario", "ID tecnico do funcionario"),
            col("empresa_id", "ID tecnico da empresa"),
            col("data_inicio", "Primeira data observada de relacionamento"),
            col("data_fim", "Ultima data observada de relacionamento"),
            col("tipo_contratacao", "Tipo de contratacao quando informado"),
            col("motivo_saida", "Motivo de saida quando informado"),
            col("status_contratacao", "Status consolidado: ATIVA ou ENCERRADA"),
            col("pipeline_run_id_origem", "ID da execucao de origem quando houver"),
        ]
    )),

    _resolve(silver_table(
        "tb_ctps",
        "Tabela silver de registros de CTPS relacionados a funcionario e empresa.",
        source=f"{CATALOG}.bronze.ctps_pdf",
        columns=[
            col("id_ctps", "ID tecnico do registro de CTPS"),
            col("id_funcionario", "ID tecnico do funcionario"),
            col("empresa_id", "ID tecnico da empresa"),
            col("nome_empresa", "Nome da empresa"),
            col("nu_cpf", "CPF normalizado do funcionario [PII]"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa"),
            col("cargo", "Cargo informado na CTPS"),
            col("tipo_contrato", "Tipo de contrato informado na CTPS"),
            col("data_admissao", "Data de admissao"),
            col("data_rescisao", "Data de rescisao"),
            col("vl_remuneracao", "Valor de remuneracao observado"),
            col("arquivo_ctps", "Arquivo de origem da CTPS"),
            col("pipeline_run_id_origem", "ID da execucao de origem quando houver"),
        ]
    )),

    _resolve(silver_table(
        "tb_cliente_referencia",
        "Tabela silver enxuta de pares consultoria-cliente com periodo provisoriamente herdado e pronta para consumo historico.",
        source=f"{CATALOG}.bronze.raw_clientes",
        columns=[
            col("cliente_referencia_id", "ID tecnico da referencia consultoria-cliente"),
            col("nm_consultoria", "Nome consolidado da consultoria"),
            col("nm_cliente", "Nome consolidado do cliente"),
            col("dt_inicio", "Data inicial conhecida ou provisoria do relacionamento consultoria-cliente"),
            col("dt_fim", "Data final conhecida ou provisoria do relacionamento consultoria-cliente"),
            col("tp_contrato", "Tipos de contrato observados no relacionamento"),
            col("fontes_origem", "Fontes usadas para montar a referencia"),
            col("observacao", "Observacao livre herdada das referencias"),
            col("fl_periodo_provisorio", "Flag indicando que o periodo ainda e provisoriamente herdado da consultoria"),
            col("arquivo_origem", "Arquivo de referencia que originou o relacionamento"),
            col("pipeline_run_id", "ID tecnico da execucao da silver"),
            col("ingested_at", "Timestamp de ingestao da silver"),
        ]
    )),

    _resolve(silver_table(
        "tb_beneficios",
        "Tabela silver de beneficios por empresa, normalizada e sem indicadores derivados.",
        source=f"{CATALOG}.bronze.raw_beneficios",
        columns=[
            col("id_beneficio", "ID tecnico da linha de beneficios"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa"),
            col("nome_empresa", "Nome preferencial da empresa"),
            col("plano_saude", "Texto original de plano de saude"),
            col("plano_odontologico", "Texto original de plano odontologico"),
            col("wellhub", "Texto original de Wellhub/Gympass"),
            col("lazer", "Texto original de lazer"),
            col("day_off_aniversario", "Texto original de day off de aniversario"),
            col("seguro_vida", "Texto original de seguro de vida"),
            col("nome_va_vr", "Nome do beneficio de VR/VA quando informado"),
            col("vr_va_valor", "Valor monetario de VR/VA quando informado"),
            col("vr_va_descricao", "Descricao textual de VR/VA quando informada"),
            col("vr_va_status", "Status textual de VR/VA quando informado"),
            col("bem_estar", "Texto original de beneficio de bem-estar"),
            col("idiomas", "Texto original de beneficio de idiomas"),
            col("plr_valor", "Valor monetario de PLR quando informado"),
            col("pipeline_run_id_origem", "ID da execucao de origem quando houver"),
            col("ingested_at_origem", "Timestamp de ingestao de origem quando houver"),
        ]
    )),

    _resolve(silver_table(
        "tb_beneficios_item",
        "Tabela silver atomica de beneficios por empresa e tipo de beneficio.",
        source=f"{CATALOG}.bronze.raw_beneficios_item",
        columns=[
            col("id_beneficio_item", "ID tecnico do item de beneficio"),
            col("empresa_id", "ID tecnico da empresa associada"),
            col("nu_cnpj_empresa", "CNPJ normalizado da empresa"),
            col("nome_empresa", "Nome preferencial da empresa"),
            col("tipo_beneficio", "Tipo canonico do beneficio"),
            col("grupo_beneficio", "Grupo funcional do beneficio"),
            col("campo_origem", "Campo raw que originou o item"),
            col("nome_beneficio", "Nome comercial do beneficio quando informado"),
            col("beneficio_descricao", "Descricao textual do beneficio"),
            col("vl_beneficio", "Valor monetario do beneficio quando informado"),
            col("status_beneficio", "Status consolidado do beneficio"),
            col("fl_beneficio_informado", "Flag indicando existencia de evidencia do beneficio"),
            col("fl_valor_informado", "Flag indicando valor monetario informado"),
            col("status_mapeamento", "Status do mapeamento da empresa na origem"),
            col("observacao_mapeamento", "Observacao do mapeamento da empresa na origem"),
            col("pipeline_run_id_origem", "ID da execucao de origem quando houver"),
            col("ingested_at_origem", "Timestamp de ingestao de origem quando houver"),
        ]
    )),

    _resolve(silver_table(
        "tb_rescisoes",
        "Tabela silver de documentos de rescisao extraidos de PDFs e normalizados.",
        source=f"{CATALOG}.bronze.rescisoes_pdf",
        columns=[
            col("id_rescisao", "ID tecnico da rescisao"),
            col("file_name", "Nome do arquivo de origem"),
            col("cpf", "CPF normalizado do profissional [PII]"),
            col("nome_profissional", "Nome do profissional extraido do documento [PII]"),
            col("nome_empresa", "Nome da empresa extraido do documento"),
            col("tipo_rescisao", "Classificacao textual da rescisao"),
            col("dt_rescisao", "Data de referencia da rescisao"),
        ]
    )),

    _resolve(silver_table(
        "tb_informe_rendimentos",
        "Tabela silver de informes de rendimentos derivados da base IRPF.",
        source=f"{CATALOG}.silver.tb_irpf",
        columns=[
            col("id_informe_rendimentos", "ID tecnico do informe de rendimentos"),
            col("cpf", "CPF normalizado do titular [PII]"),
            col("nome", "Nome do titular informado na origem [PII]"),
            col("exercicio", "Exercicio fiscal do informe"),
            col("ano_calendario", "Ano-calendario de referencia"),
            col("rendimentos_tributaveis", "Valor de rendimentos tributaveis quando extraido"),
            col("imposto_retido_total", "Valor total de imposto retido ou pago"),
            col("imposto_restituir", "Valor total a restituir quando informado"),
            col("pipeline_run_id_origem", "ID da execucao de origem quando houver"),
            col("ingested_at_origem", "Timestamp de ingestao de origem quando houver"),
        ]
    )),

    _resolve(silver_table(
        "tb_fgts",
        "Tabela silver de eventos de FGTS normalizados e deduplicados por competencia e lancamento.",
        source=f"{CATALOG}.bronze.fgts_extratos",
        columns=[
            col("id_fgts", "ID tecnico do evento de FGTS"),
            col("nome", "Nome do titular informado no cabecalho do extrato [PII]"),
            col("nr_cnpj_contratante", "CNPJ da empresa associada ao evento"),
            col("nome_empresa", "Nome da empresa associado ao evento"),
            col("nr_conta_fgts", "Numero da conta FGTS"),
            col("nr_pis_pasep", "Numero do PIS/PASEP [PII]"),
            col("dt_lancamento", "Data do lancamento do evento"),
            col("competencia", "Competencia mensal do evento"),
            col("tp_evento", "Tipo de evento do FGTS"),
            col("valor", "Valor do evento"),
            col("saldo", "Saldo informado apos o evento"),
        ]
    )),

    _resolve(silver_table(
        "tb_inss",
        "Tabela silver de INSS consolidada por CPF, empresa e competencia.",
        source=f"{CATALOG}.bronze.inss_pdf",
        columns=[
            col("id_inss", "ID tecnico do registro de INSS"),
            col("cpf", "CPF normalizado do titular [PII]"),
            col("nit", "NIT do titular [PII]"),
            col("nome", "Nome do titular [PII]"),
            col("dt_nascimento", "Data de nascimento do titular [PII]"),
            col("nome_mae", "Nome da mae do titular [PII]"),
            col("nr_pis_pasep", "Numero do PIS/PASEP [PII]"),
            col("tp_filiado", "Tipo de filiacao previdenciaria"),
            col("nu_cnpj", "CNPJ da empresa associado ao registro"),
            col("nome_empresa", "Nome da empresa associado ao registro"),
            col("seq", "Sequencial do vinculo na origem"),
            col("dt_inicio", "Data de inicio do vinculo"),
            col("dt_fim", "Data final do vinculo"),
            col("competencia", "Competencia mensal do registro"),
            col("vl_remuneracao", "Valor de remuneracao da competencia"),
            col("vl_contribuicao_inss", "Valor da contribuicao do INSS"),
            col("doc_id", "Documento de origem principal do registro"),
        ]
    )),

    _resolve(silver_table(
        "tb_das",
        "Tabela silver de guias DAS deduplicadas e consolidadas por documento.",
        source=f"{CATALOG}.bronze.das",
        columns=[
            col("id_das", "ID tecnico da guia DAS"),
            col("cnpj", "CNPJ da empresa da guia"),
            col("razao_social", "Razao social informada na guia"),
            col("competencia", "Competencia mensal da guia"),
            col("nr_documento", "Numero do documento da guia"),
            col("tp_lancamento", "Tipo de lancamento da guia"),
            col("dt_vencimento", "Data de vencimento da guia"),
            col("dt_arrecadacao", "Data de arrecadacao da guia"),
            col("vl_principal", "Valor principal da guia"),
            col("vl_multa", "Valor de multa da guia"),
            col("vl_juros", "Valor de juros da guia"),
            col("vl_total", "Valor total da guia"),
        ]
    )),

    _resolve(silver_table(
        "tb_notas_fiscais",
        (
            "Tabela silver de notas fiscais PORTFOLIO/PROVIDER_A unificadas, deduplicadas "
            "e enriquecidas com empresa prestadora/tomadora."
        ),
        source=f"{CATALOG}.bronze.nf_provider_a|{CATALOG}.bronze.nf_portfolio",
        columns=[
            col("id_nota_fiscal", "ID tecnico da nota fiscal"),
            col("fonte", "Fonte de origem da nota fiscal"),
            col("nf_numero", "Numero da NFS-e"),
            col("competencia", "Competencia mensal da nota fiscal"),
            col("data_emissao", "Data de emissao da nota fiscal"),
            col("prestador_empresa_id", "ID tecnico da empresa prestadora"),
            col("prestador_cnpj", "CNPJ da empresa prestadora"),
            col("tomador_empresa_id", "ID tecnico da empresa tomadora quando houver match"),
            col("tomador_cnpj", "CNPJ da empresa tomadora"),
            col("tomador_nome_empresa", "Nome canonico ou informado da empresa tomadora"),
            col("valor_servicos", "Valor bruto dos servicos"),
            col("valor_liquido", "Valor liquido apos deducoes"),
            col("valor_deducoes", "Valor de deducoes da nota fiscal"),
            col("valor_iss", "Valor de ISS destacado na nota fiscal"),
            col("iss_retido", "Indicador de ISS retido na fonte"),
        ]
    )),

    _resolve(silver_table(
        "tb_irpf",
        "Tabela silver de declaracoes de IRPF deduplicadas por CPF, exercicio e ano-calendario.",
        source=f"{CATALOG}.bronze.irpf_declaracoes",
        columns=[
            col("id_irpf", "ID tecnico da declaracao de IRPF"),
            col("cpf", "CPF normalizado do titular [PII]"),
            col("nome", "Nome do titular [PII]"),
            col("exercicio", "Exercicio fiscal da declaracao"),
            col("ano_calendario", "Ano-calendario de referencia"),
            col("tipo_declaracao", "Tipo da declaracao informada na origem"),
            col("imposto_pago_total", "Valor total de imposto pago ou retido"),
            col("imposto_restituir", "Valor total a restituir"),
        ]
    )),

    # Bronze active
    _resolve(bronze_table("empresas", "Raw - Dados brutos de empresas agregados de multiplas fontes.")),
    _resolve(bronze_table("raw_beneficios", "Raw - Beneficios ofertados por empresa no formato largo de aterrissagem.")),
    _resolve(bronze_table("raw_beneficios_item", "Raw - Beneficios itemizados por empresa e tipo de beneficio.")),
    _resolve(bronze_table("contratos_geral", "Raw - Contratos de trabalho consolidados de multiplas fontes.")),
    _resolve(bronze_table("ctps_pdf", "Raw - Vinculos extraidos de CTPS em PDF.", format_type="pdf_extraction")),
    _resolve(bronze_table("fgts_extratos", "Raw - Extratos e eventos de FGTS extraidos de arquivos brutos.")),
    _resolve(bronze_table("inss_pdf", "Raw - Vinculos e remuneracoes INSS extraidos de PDFs.", format_type="pdf_extraction")),
    _resolve(bronze_table("rescisoes_pdf", "Raw - Documentos de rescisao extraidos de PDFs para JSONL.", format_type="jsonl")),
    _resolve(bronze_table("das", "Raw - Guias DAS extraidas de PDFs.", format_type="pdf_extraction")),
    _resolve(bronze_table("irpf_declaracoes", "Raw - Declaracoes IRPF extraidas de PDFs.", format_type="jsonl")),
    _resolve(bronze_table("reunioes_contratacao", "Raw - Levantamento consolidado de reunioes de contratacao importado de planilha Excel.")),
    _resolve(bronze_table("funcionarios_levantamento", "Raw - Levantamento de funcionarios importado de planilha Excel.")),
    _resolve(bronze_table("nf_provider_a", "Raw - Notas fiscais PROVIDER_A importadas sem transformacoes analiticas.")),
    _resolve(bronze_table("nf_portfolio", "Raw - Notas fiscais PORTFOLIO importadas sem transformacoes analiticas.")),
]


_METADATA_INDEX: dict[str, dict] = {t["table_name"]: t for t in TABLE_METADATA}


def _escape(text: str) -> str:
    return text.replace("'", "''")


def _apply_table_metadata(table: dict, dry_run: bool = False) -> None:
    full_name = table["table_name"]
    description = table.get("table_description", "")
    tags = table.get("tags", {})
    columns = table.get("columns", [])

    print(f"  -> {full_name}")

    if description:
        sql = f"COMMENT ON TABLE {full_name} IS '{_escape(description)}'"
        if dry_run:
            print(f"     [DRY-RUN] {sql[:120]}")
        else:
            spark.sql(sql)

    if tags:
        props = ", ".join(f"'{k}' = '{_escape(str(v))}'" for k, v in tags.items())
        sql = f"ALTER TABLE {full_name} SET TBLPROPERTIES ({props})"
        if dry_run:
            print(f"     [DRY-RUN] {sql[:120]}")
        else:
            spark.sql(sql)

    for col_def in columns:
        sql = (
            f"ALTER TABLE {full_name} "
            f"ALTER COLUMN {col_def['name']} "
            f"COMMENT '{_escape(col_def['comment'])}'"
        )
        if dry_run:
            print(f"     [DRY-RUN] col={col_def['name']}")
        else:
            spark.sql(sql)


def apply_governance(table_name: str, dry_run: bool = False) -> None:
    """Apply governance metadata for a single table."""
    table = _METADATA_INDEX.get(table_name)
    if table is None:
        print(f"[WARN] apply_governance: '{table_name}' not registered in catalog_metadata - skipping.")
        return

    try:
        _apply_table_metadata(table, dry_run=dry_run)
    except Exception as e:
        msg = str(e).lower()
        if "table_or_view_not_found" in msg or "does not exist" in msg:
            raise RuntimeError(
                f"[governance] Table '{table_name}' not found after write - pipeline wrote nothing?"
            ) from e
        print(f"[WARN] Governance partially failed for '{table_name}': {e}")


def apply_all_governance(dry_run: bool = False) -> None:
    """Apply governance metadata for all registered active tables."""
    mode = "DRY-RUN" if dry_run else "APPLY"
    print(f"[catalog_metadata] {mode} - catalog={CATALOG} env={ENV}")
    print(f"[catalog_metadata] {len(TABLE_METADATA)} tables registered\n")

    errors = []
    for table in TABLE_METADATA:
        try:
            _apply_table_metadata(table, dry_run=dry_run)
        except Exception as e:
            errors.append((table["table_name"], str(e)))
            print(f"     [ERROR] {e}")

    print(f"\n{'=' * 60}")
    if errors:
        print(f"[catalog_metadata] Finished with {len(errors)} error(s):")
        for name, err in errors:
            print(f"  - {name}: {err}")
        print("=" * 60)
        raise RuntimeError(f"apply_all_governance failed for {len(errors)} table(s): {[n for n, _ in errors]}")

    print(f"[catalog_metadata] Done. {len(TABLE_METADATA)} tables processed.")
    print("=" * 60)
