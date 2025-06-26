SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', closed_time) as closed_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    SAFE_CAST(UPPER(ativo) AS BOOL) as ativo,
    nome,
    apelido,
    SAFE.PARSE_DATE('%d/%m/%Y', nascimento) as nascimento,
    genero,
    email,
    cpf,
    telefone,
    endereco,
    bairro,
    cargo
FROM {{ source('planilhas', 'funcionarios') }}