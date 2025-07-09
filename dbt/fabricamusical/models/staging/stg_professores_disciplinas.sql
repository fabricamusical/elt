SELECT
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    created_by,
    professor_id,
    disciplina_id
FROM {{ source('planilhas', 'professores_disciplinas') }}