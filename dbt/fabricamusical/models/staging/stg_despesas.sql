SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', closed_time) as closed_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    SAFE_CAST(UPPER(valido) AS BOOL) as valido,
    tipo,
    SAFE.PARSE_DATE('%d/%m/%Y', data_pagamento) as data_pagamento,
    SAFE_CAST(replace(valor, ',','.') as NUMERIC) as valor,
    descricao,
    funcionario_id,
FROM {{ source('planilhas', 'despesas') }}