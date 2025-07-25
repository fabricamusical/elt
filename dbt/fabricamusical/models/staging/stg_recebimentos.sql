SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', closed_time) as closed_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    SAFE_CAST(UPPER(valido) AS BOOL) as valido,
    tipo,
    matricula,
    SAFE_CAST(replace(valor_base, ',','.') as NUMERIC) as valor_base,
    SAFE_CAST(replace(desconto_valor, ',','.') as NUMERIC) as desconto_valor,
    desconto_tipo_id,
    SAFE.PARSE_DATE('%d/%m/%Y', data_pagamento) as data_pagamento,
    SAFE.PARSE_DATE('%d/%m/%Y', mes_referencia) as mes_referencia,
    forma_pagamento,
    observacoes,
    SAFE_CAST(UPPER(atrasado) AS BOOL) as atrasado,
    SAFE_CAST(replace(valor_pago, ',','.') as NUMERIC) as valor_pago
FROM {{ source('planilhas', 'recebimentos') }}