SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by
    nome,
    tipo,
    SAFE_CAST(UPPER(ativa) AS BOOL) as ativa
FROM {{ source('planilhas', 'disciplinas') }}
    
    