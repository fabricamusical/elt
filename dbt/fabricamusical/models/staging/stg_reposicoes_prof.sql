SELECT
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', carimbo_de_data_hora) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    SAFE.PARSE_DATE('%d/%m/%Y', data_reposicao) as data_reposicao,
    aluno,
    horario,
    motivo,
    SAFE.PARSE_DATE('%d/%m/%Y', data_aula_desmarcada) as data_aula_desmarcada,
    sala,
    endereco_de_email,
    SAFE_CAST(UPPER(valido) AS BOOL) as valido,
    professor_id,
    updated_by,
    SAFE_CAST(UPPER(verificada) AS BOOL) as verificada,
    dia_semana    
FROM {{ source('planilhas', 'reposicoes_prof') }}