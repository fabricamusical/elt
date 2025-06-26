SELECT
    id,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', created_time) as created_time,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', closed_time) as closed_time,
    created_by,
    SAFE.PARSE_DATETIME('%d/%m/%Y %H:%M:%S', last_update) as last_update,
    updated_by,
    aluno_id,
    desconto_id,
    professor_id,
    disciplina_id,
    horario,
    dia_semana,
    sala,
    CAST(dia_faturamento AS INT) as dia_faturamento,
    tipo,
    numero_alunos,
    observacoes,
    SAFE_CAST(UPPER(ativa) AS BOOL) as ativa,
    motivo_saida
    FROM {{ source('planilhas', 'matriculas')}}
    