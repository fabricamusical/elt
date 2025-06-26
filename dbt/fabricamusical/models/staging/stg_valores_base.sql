SELECT
    tipo,
    SAFE_CAST(replace(valor, ',','.') as NUMERIC) as valor,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', updated_time) as updated_time,
    updated_by
FROM {{ source('planilhas', 'valores_base')}}