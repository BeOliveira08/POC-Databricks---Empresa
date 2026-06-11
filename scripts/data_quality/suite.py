"""Suite declarativa de checks para o schema atual do Portfolio Lakehouse Template.

Objetivo:
    - Validar rapidamente tabelas centrais de silver/gold
    - Pegar regressao de chave, formato e coerencia temporal
    - Ficar enxuta o suficiente para rodar sempre
"""

from .checks import Expression, NotNull, RefIntegrity, RowCountMin, Unique, UniqueComposite


SUITE = [
    # silver.tb_empresa
    RowCountMin(
        table="silver.tb_empresa",
        min_rows=1,
        display_name="tb_empresa: tabela tem dados",
    ),
    NotNull(
        table="silver.tb_empresa",
        col="empresa_id",
        display_name="tb_empresa: empresa_id preenchido",
    ),
    NotNull(
        table="silver.tb_empresa",
        col="nm_empresa",
        display_name="tb_empresa: nome preenchido",
    ),
    Unique(
        table="silver.tb_empresa",
        col="empresa_id",
        display_name="tb_empresa: empresa_id unico",
    ),
    Expression(
        table="silver.tb_empresa",
        rule="nu_cnpj IS NULL OR nu_cnpj RLIKE '^[0-9]{14}$'",
        name="tb_empresa.nu_cnpj_formato",
        display_name="tb_empresa: CNPJ com 14 digitos quando existir",
    ),
    Expression(
        table="silver.tb_empresa",
        rule="data_inicio IS NULL OR data_fim IS NULL OR data_fim >= data_inicio",
        name="tb_empresa.periodo_coerente",
        display_name="tb_empresa: periodo coerente",
    ),

    # silver.tb_cliente_referencia
    RowCountMin(
        table="silver.tb_cliente_referencia",
        min_rows=1,
        display_name="tb_cliente_referencia: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="silver.tb_cliente_referencia",
        col="cliente_referencia_id",
        display_name="tb_cliente_referencia: id preenchido",
        severity="warn",
    ),
    Unique(
        table="silver.tb_cliente_referencia",
        col="cliente_referencia_id",
        display_name="tb_cliente_referencia: id unico",
        severity="warn",
    ),
    NotNull(
        table="silver.tb_cliente_referencia",
        col="nm_cliente",
        display_name="tb_cliente_referencia: cliente preenchido",
        severity="warn",
    ),
    UniqueComposite(
        table="silver.tb_cliente_referencia",
        cols=["nm_consultoria", "nm_cliente"],
        display_name="tb_cliente_referencia: par consultoria-cliente unico",
        severity="warn",
    ),
    Expression(
        table="silver.tb_cliente_referencia",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="tb_cliente_referencia.periodo_coerente",
        display_name="tb_cliente_referencia: periodo coerente",
        severity="warn",
    ),

    # silver.tb_ctps
    RowCountMin(
        table="silver.tb_ctps",
        min_rows=1,
        display_name="tb_ctps: tabela tem dados",
    ),
    NotNull(
        table="silver.tb_ctps",
        col="id_ctps",
        display_name="tb_ctps: id preenchido",
    ),
    Unique(
        table="silver.tb_ctps",
        col="id_ctps",
        display_name="tb_ctps: id unico",
    ),
    Expression(
        table="silver.tb_ctps",
        rule="nu_cpf IS NULL OR nu_cpf RLIKE '^[0-9]{11}$'",
        name="tb_ctps.nu_cpf_formato",
        display_name="tb_ctps: CPF com 11 digitos quando existir",
    ),
    Expression(
        table="silver.tb_ctps",
        rule="nu_cnpj_empresa IS NULL OR nu_cnpj_empresa RLIKE '^[0-9]{14}$'",
        name="tb_ctps.nu_cnpj_empresa_formato",
        display_name="tb_ctps: CNPJ da empresa com 14 digitos quando existir",
    ),
    Expression(
        table="silver.tb_ctps",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="tb_ctps.periodo_coerente",
        display_name="tb_ctps: periodo coerente",
    ),

    # silver.tb_contratos
    RowCountMin(
        table="silver.tb_contratos",
        min_rows=1,
        display_name="tb_contratos: tabela tem dados",
    ),
    NotNull(
        table="silver.tb_contratos",
        col="id_contrato",
        display_name="tb_contratos: id preenchido",
    ),
    Unique(
        table="silver.tb_contratos",
        col="id_contrato",
        display_name="tb_contratos: id unico",
    ),
    NotNull(
        table="silver.tb_contratos",
        col="dt_inicio",
        display_name="tb_contratos: data inicial preenchida",
    ),
    Expression(
        table="silver.tb_contratos",
        rule="cnpj_consultoria IS NULL OR cnpj_consultoria RLIKE '^[0-9]{14}$'",
        name="tb_contratos.cnpj_consultoria_formato",
        display_name="tb_contratos: CNPJ da consultoria com 14 digitos quando existir",
        severity="warn",
    ),
    Expression(
        table="silver.tb_contratos",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="tb_contratos.periodo_coerente",
        display_name="tb_contratos: periodo coerente",
    ),

    # silver.tb_inss
    RowCountMin(
        table="silver.tb_inss",
        min_rows=1,
        display_name="tb_inss: tabela tem dados",
    ),
    NotNull(
        table="silver.tb_inss",
        col="id_inss",
        display_name="tb_inss: id preenchido",
    ),
    Unique(
        table="silver.tb_inss",
        col="id_inss",
        display_name="tb_inss: id unico",
    ),
    Expression(
        table="silver.tb_inss",
        rule="nu_cpf IS NULL OR nu_cpf RLIKE '^[0-9]{11}$'",
        name="tb_inss.nu_cpf_formato",
        display_name="tb_inss: CPF com 11 digitos quando existir",
    ),
    Expression(
        table="silver.tb_inss",
        rule="nu_cnpj_empresa IS NULL OR nu_cnpj_empresa RLIKE '^[0-9]{14}$'",
        name="tb_inss.nu_cnpj_empresa_formato",
        display_name="tb_inss: CNPJ com 14 digitos quando existir",
        severity="warn",
    ),
    Expression(
        table="silver.tb_inss",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="tb_inss.periodo_coerente",
        display_name="tb_inss: periodo coerente",
    ),

    # silver.tb_fgts
    RowCountMin(
        table="silver.tb_fgts",
        min_rows=1,
        display_name="tb_fgts: tabela tem dados",
    ),
    NotNull(
        table="silver.tb_fgts",
        col="id_fgts",
        display_name="tb_fgts: id preenchido",
    ),
    Unique(
        table="silver.tb_fgts",
        col="id_fgts",
        display_name="tb_fgts: id unico",
    ),
    NotNull(
        table="silver.tb_fgts",
        col="dt_lancamento",
        display_name="tb_fgts: data de lancamento preenchida",
    ),
    NotNull(
        table="silver.tb_fgts",
        col="competencia",
        display_name="tb_fgts: competencia preenchida",
    ),
    Expression(
        table="silver.tb_fgts",
        rule="nr_cnpj_contratante IS NULL OR nr_cnpj_contratante RLIKE '^[0-9]{14}$'",
        name="tb_fgts.nr_cnpj_contratante_formato",
        display_name="tb_fgts: CNPJ da contratante com 14 digitos quando existir",
        severity="warn",
    ),

    # gold.dim_empresa
    RowCountMin(
        table="gold.dim_empresa",
        min_rows=1,
        display_name="dim_empresa: tabela tem dados",
    ),
    NotNull(
        table="gold.dim_empresa",
        col="empresa_id",
        display_name="dim_empresa: empresa_id preenchido",
    ),
    NotNull(
        table="gold.dim_empresa",
        col="nm_empresa",
        display_name="dim_empresa: nome preenchido",
    ),
    Unique(
        table="gold.dim_empresa",
        col="empresa_id",
        display_name="dim_empresa: empresa_id unico",
    ),
    RefIntegrity(
        fact="gold.dim_empresa",
        fact_col="empresa_id",
        dim="silver.tb_empresa",
        dim_col="empresa_id",
        name="dim_empresa.empresa_id_existe_em_tb_empresa",
        display_name="dim_empresa: empresa_id existe em tb_empresa",
    ),
    Expression(
        table="gold.dim_empresa",
        rule="nu_cnpj IS NULL OR nu_cnpj RLIKE '^[0-9]{14}$'",
        name="dim_empresa.nu_cnpj_formato",
        display_name="dim_empresa: CNPJ com 14 digitos quando existir",
    ),
    Expression(
        table="gold.dim_empresa",
        rule="data_inicio IS NULL OR data_fim IS NULL OR data_fim >= data_inicio",
        name="dim_empresa.periodo_coerente",
        display_name="dim_empresa: periodo coerente",
    ),
    Expression(
        table="gold.dim_empresa",
        rule="""
NOT EXISTS (
    SELECT 1
    FROM (
        SELECT regexp_replace(
            regexp_replace(upper(coalesce(nm_empresa, '')), '[^A-Z0-9 ]', ' '),
            '\\s+',
            ''
        ) AS empresa_nome_key
        FROM {catalog}.gold.dim_empresa
        WHERE nm_empresa IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) > 1
    ) d
    WHERE regexp_replace(
        regexp_replace(upper(coalesce(nm_empresa, '')), '[^A-Z0-9 ]', ' '),
        '\\s+',
        ''
    ) = d.empresa_nome_key
)
""".strip(),
        name="dim_empresa.nome_normalizado_sem_duplicidade",
        display_name="dim_empresa: sem nomes normalizados duplicados",
        severity="warn",
    ),

    # gold.dim_cliente
    RowCountMin(
        table="gold.dim_cliente",
        min_rows=1,
        display_name="dim_cliente: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.dim_cliente",
        col="cliente_id",
        display_name="dim_cliente: cliente_id preenchido",
        severity="warn",
    ),
    NotNull(
        table="gold.dim_cliente",
        col="nm_cliente",
        display_name="dim_cliente: nome preenchido",
        severity="warn",
    ),
    Unique(
        table="gold.dim_cliente",
        col="cliente_id",
        display_name="dim_cliente: cliente_id unico",
        severity="warn",
    ),
    Expression(
        table="gold.dim_cliente",
        rule="""
NOT EXISTS (
    SELECT 1
    FROM (
        SELECT regexp_replace(upper(coalesce(nm_cliente, '')), '[^A-Z0-9]', '') AS cliente_nome_key
        FROM {catalog}.gold.dim_cliente
        WHERE nm_cliente IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) > 1
    ) d
    WHERE regexp_replace(upper(coalesce(nm_cliente, '')), '[^A-Z0-9]', '') = d.cliente_nome_key
)
""".strip(),
        name="dim_cliente.nome_normalizado_sem_duplicidade",
        display_name="dim_cliente: sem nomes normalizados duplicados",
        severity="warn",
    ),

    # gold.fato_ctps
    RowCountMin(
        table="gold.fato_ctps",
        min_rows=1,
        display_name="fato_ctps: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_ctps",
        col="sk_ctps",
        display_name="fato_ctps: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_ctps",
        col="sk_ctps",
        display_name="fato_ctps: chave unica",
        severity="warn",
    ),
    RefIntegrity(
        fact="gold.fato_ctps",
        fact_col="empresa_id",
        dim="gold.dim_empresa",
        dim_col="empresa_id",
        name="fato_ctps.empresa_id_existe_em_dim_empresa",
        display_name="fato_ctps: empresa existe na dim_empresa",
        severity="warn",
    ),
    Expression(
        table="gold.fato_ctps",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="fato_ctps.periodo_coerente",
        display_name="fato_ctps: periodo coerente",
        severity="warn",
    ),
    Expression(
        table="gold.fato_ctps",
        rule="dt_fim IS NULL OR dt_fim > DATE '1900-01-01'",
        name="fato_ctps.dt_fim_sem_sentinela",
        display_name="fato_ctps: dt_fim sem data sentinela",
        severity="warn",
    ),

    # gold.fato_fgts
    RowCountMin(
        table="gold.fato_fgts",
        min_rows=1,
        display_name="fato_fgts: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_fgts",
        col="sk_fgts",
        display_name="fato_fgts: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_fgts",
        col="sk_fgts",
        display_name="fato_fgts: chave unica",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_fgts",
        col="competencia",
        display_name="fato_fgts: competencia preenchida",
        severity="warn",
    ),
    RefIntegrity(
        fact="gold.fato_fgts",
        fact_col="empresa_id",
        dim="gold.dim_empresa",
        dim_col="empresa_id",
        name="fato_fgts.empresa_id_existe_em_dim_empresa",
        display_name="fato_fgts: empresa existe na dim_empresa",
        severity="warn",
    ),
    Expression(
        table="gold.fato_fgts",
        rule="valor_fgts IS NULL OR valor_fgts >= 0",
        name="fato_fgts.valor_fgts_nao_negativo",
        display_name="fato_fgts: valor FGTS nao negativo",
        severity="warn",
    ),
    Expression(
        table="gold.fato_fgts",
        rule="valor_depositado IS NULL OR valor_depositado >= 0",
        name="fato_fgts.valor_depositado_nao_negativo",
        display_name="fato_fgts: valor depositado nao negativo",
        severity="warn",
    ),

    # gold.fato_vinculo_empresa
    RowCountMin(
        table="gold.fato_vinculo_empresa",
        min_rows=1,
        display_name="fato_vinculo_empresa: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_vinculo_empresa",
        col="sk_vinculo",
        display_name="fato_vinculo_empresa: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_vinculo_empresa",
        col="sk_vinculo",
        display_name="fato_vinculo_empresa: chave unica",
        severity="warn",
    ),
    RefIntegrity(
        fact="gold.fato_vinculo_empresa",
        fact_col="empresa_id",
        dim="gold.dim_empresa",
        dim_col="empresa_id",
        name="fato_vinculo_empresa.empresa_id_existe_em_dim_empresa",
        display_name="fato_vinculo_empresa: empresa existe na dim_empresa",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="dt_inicio IS NULL OR dt_fim IS NULL OR dt_fim >= dt_inicio",
        name="fato_vinculo_empresa.periodo_vinculo_coerente",
        display_name="fato_vinculo_empresa: periodo do vinculo coerente",
        severity="warn",
    ),
    Unique(
        table="gold.fato_vinculo_empresa",
        col="funcionario_empresa_id",
        display_name="fato_vinculo_empresa: funcionario_empresa_id unico",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="dt_inicio_cliente IS NULL OR dt_fim_cliente IS NULL OR dt_fim_cliente >= dt_inicio_cliente",
        name="fato_vinculo_empresa.periodo_cliente_coerente",
        display_name="fato_vinculo_empresa: periodo do cliente coerente",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="dt_fim IS NULL OR dt_fim > DATE '1900-01-01'",
        name="fato_vinculo_empresa.dt_fim_sem_sentinela",
        display_name="fato_vinculo_empresa: dt_fim sem data sentinela",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="dt_fim_cliente IS NULL OR dt_fim_cliente > DATE '1900-01-01'",
        name="fato_vinculo_empresa.dt_fim_cliente_sem_sentinela",
        display_name="fato_vinculo_empresa: dt_fim_cliente sem data sentinela",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="cliente_referencia_id IS NULL OR cliente_id IS NOT NULL",
        name="fato_vinculo_empresa.cliente_referenciado_resolvido",
        display_name="fato_vinculo_empresa: cliente referenciado resolve cliente_id",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="""
dt_inicio_cliente IS NULL
OR dt_inicio IS NULL
OR COALESCE(dt_fim_cliente, DATE '2999-12-31') >= dt_inicio
""".strip(),
        name="fato_vinculo_empresa.inicio_cliente_sobrepoe_vinculo",
        display_name="fato_vinculo_empresa: inicio cliente sobrepoe vinculo",
        severity="warn",
    ),
    Expression(
        table="gold.fato_vinculo_empresa",
        rule="""
dt_inicio_cliente IS NULL
OR COALESCE(dt_fim, DATE '2999-12-31') >= dt_inicio_cliente
""".strip(),
        name="fato_vinculo_empresa.fim_vinculo_sobrepoe_cliente",
        display_name="fato_vinculo_empresa: fim do vinculo sobrepoe periodo cliente",
        severity="warn",
    ),
    RefIntegrity(
        fact="gold.fato_vinculo_empresa",
        fact_col="cliente_id",
        dim="gold.dim_cliente",
        dim_col="cliente_id",
        name="fato_vinculo_empresa.cliente_id_existe_em_dim_cliente",
        display_name="fato_vinculo_empresa: cliente existe na dim_cliente",
        severity="warn",
    ),

    # gold.fato_notas_fiscais
    RowCountMin(
        table="gold.fato_notas_fiscais",
        min_rows=1,
        display_name="fato_notas_fiscais: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_notas_fiscais",
        col="sk_nota_fiscal",
        display_name="fato_notas_fiscais: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_notas_fiscais",
        col="sk_nota_fiscal",
        display_name="fato_notas_fiscais: chave unica",
        severity="warn",
    ),
    Expression(
        table="gold.fato_notas_fiscais",
        rule="prestador_empresa_id IS NOT NULL",
        name="fato_notas_fiscais.prestador_resolvido",
        display_name="fato_notas_fiscais: prestador resolvido",
        severity="warn",
    ),
    Expression(
        table="gold.fato_notas_fiscais",
        rule="tomador_empresa_id IS NOT NULL",
        name="fato_notas_fiscais.tomador_resolvido",
        display_name="fato_notas_fiscais: tomador resolvido",
        severity="warn",
    ),

    # gold.fato_das
    RowCountMin(
        table="gold.fato_das",
        min_rows=1,
        display_name="fato_das: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_das",
        col="sk_das",
        display_name="fato_das: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_das",
        col="sk_das",
        display_name="fato_das: chave unica",
        severity="warn",
    ),
    Expression(
        table="gold.fato_das",
        rule="""
NOT EXISTS (
    SELECT 1
    FROM (
        SELECT empresa_id, competencia, nr_documento
        FROM {catalog}.gold.fato_das
        WHERE empresa_id IS NOT NULL
          AND competencia IS NOT NULL
          AND nr_documento IS NOT NULL
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
    ) d
    WHERE d.empresa_id = empresa_id
      AND d.competencia = competencia
      AND d.nr_documento = nr_documento
)
""".strip(),
        name="fato_das.grao_documento_unico",
        display_name="fato_das: sem duplicidade por empresa-competencia-documento",
        severity="warn",
    ),

    # gold.fato_empresa_indicadores
    RowCountMin(
        table="gold.fato_empresa_indicadores",
        min_rows=1,
        display_name="fato_empresa_indicadores: tabela tem dados",
        severity="warn",
    ),
    NotNull(
        table="gold.fato_empresa_indicadores",
        col="sk_empresa_indicador",
        display_name="fato_empresa_indicadores: chave preenchida",
        severity="warn",
    ),
    Unique(
        table="gold.fato_empresa_indicadores",
        col="sk_empresa_indicador",
        display_name="fato_empresa_indicadores: chave unica",
        severity="warn",
    ),
    Expression(
        table="gold.fato_empresa_indicadores",
        rule="qt_dias_permanencia_total IS NULL OR qt_dias_permanencia_total >= 0",
        name="fato_empresa_indicadores.dias_total_nao_negativo",
        display_name="fato_empresa_indicadores: dias totais nao negativos",
        severity="warn",
    ),
    Expression(
        table="gold.fato_empresa_indicadores",
        rule="qt_dias_permanencia_media IS NULL OR qt_dias_permanencia_media >= 0",
        name="fato_empresa_indicadores.dias_media_nao_negativo",
        display_name="fato_empresa_indicadores: dias medios nao negativos",
        severity="warn",
    ),
    Expression(
        table="gold.fato_empresa_indicadores",
        rule="qt_meses_permanencia_media IS NULL OR qt_meses_permanencia_media >= 0",
        name="fato_empresa_indicadores.meses_media_nao_negativo",
        display_name="fato_empresa_indicadores: meses medios nao negativos",
        severity="warn",
    ),
]
