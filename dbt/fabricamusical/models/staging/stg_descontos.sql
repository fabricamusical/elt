SELECT
    id,
    nome,
    descricao,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    SAFE_CAST(UPPER(ativo) AS BOOL) as ativo,
    SAFE_CAST(replace(TRIM(percentual, '%'), ',', '.') AS NUMERIC)/100 AS percentual,
    updated_by
FROM {{ source('planilhas', 'descontos') }}

