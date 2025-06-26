with base as
(
SELECT 
    c.id as cliente_id, 
    c.tipo as cliente_tipo, 
    c.nascimento, 
    DATE_DIFF(CURRENT_DATE(), c.nascimento, YEAR) as idade,
    c.genero, 
    c.bairro, 
    c.conheceu_escola, 
    c.conhecimento_musical, 
    m.id as matricula_id, 
    r.valor_pago as valor_pago, 
    if(r.atrasado = TRUE, 1, 0) as atrasado 
FROM {{ ref('stg_clientes') }} c 
JOIN {{ ref('stg_matriculas')}} m ON c.id = m.aluno_id and m.ativa = TRUE
LEFT JOIN {{ ref('stg_recebimentos' )}} r ON r.matricula = m.id )


SELECT cliente_id, cliente_tipo, nascimento, idade, genero, bairro, conheceu_escola, conhecimento_musical,
count(distinct(matricula_id)) as matriculas_ativas,
sum(valor_pago) as total_pago,
sum(atrasado) as pagamentos_atrasados,
count(*) as pagamentos_realizados
FROM base
GROUP BY cliente_id, cliente_tipo, nascimento, idade, genero, bairro, conheceu_escola, conhecimento_musical