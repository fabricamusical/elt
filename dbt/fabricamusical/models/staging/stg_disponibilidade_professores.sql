SELECT
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    created_by,
    professor_id,
    SAFE.PARSE_DATE('%d/%m/%Y', dia) as dia,
    horario
FROM {{ source('planilhas', 'disponibilidade_professores') }}