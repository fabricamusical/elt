SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    nome,
    tipo,
    SAFE.PARSE_DATE('%d/%m/%Y', nascimento) as nascimento,
    genero,
    email,
    cpf,
    telefone,
    endereco,
    bairro,
    conheceu_escola,
    conhecimento_musical,
    telefone_alternativo,
    responsavel,
    apelido
    FROM {{ source('planilhas', 'clientes')}}
    