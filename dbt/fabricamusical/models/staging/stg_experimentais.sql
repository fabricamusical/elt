SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', closed_time) as closed_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    SAFE_CAST(UPPER(valido) AS BOOL) as valido,
    aluno_id,
    professor_id,
    disciplina_id,
    horario,
    dia_semana,
    sala,
    status,
    SAFE.PARSE_DATE('%d/%m/%Y', data) as data
FROM {{ source('planilhas', 'experimentais') }}