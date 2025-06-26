select 
alunos.apelido as aluno, 
alunos.tipo as tipo_de_aluno,
responsaveis.apelido as responsavel,
case when responsaveis.id is null then alunos.telefone else responsaveis.telefone end as telefone,
case when responsaveis.id is null then alunos.telefone_alternativo else responsaveis.telefone_alternativo end as telefone2,
professores.apelido as professor, 
disciplinas.nome as disciplina, 
concat(matriculas.tipo, if(matriculas.tipo = 'Instrumento', concat(' - ', matriculas.numero_alunos), '')) as tipo_de_aula,
case 
when matriculas.tipo = 'Prática de conjunto' then (select valor from {{ source('planilhas', 'valores_base') }} where tipo = 'Mensalidade - Prática de conjunto')
when matriculas.tipo = 'Instrumento' and matriculas.numero_alunos = '1' then (select valor from {{ source('planilhas', 'valores_base') }} where tipo = 'Mensalidade - Instrumento individual')
when matriculas.tipo = 'Instrumento' and matriculas.numero_alunos = '2' then (select valor from {{ source('planilhas', 'valores_base') }} where tipo = 'Mensalidade - Instrumento em dupla')
else null end as valor_devido,
descontos.percentual as desconto,
matriculas.dia_faturamento as dia_vencimento

from {{ source('planilhas', 'matriculas') }} matriculas
left join {{ source('planilhas', 'recebimentos') }} recebimentos on recebimentos.tipo = 'Mensalidade' 
    and recebimentos.matricula = matriculas.id  
    and recebimentos.valido IN ('true', 'TRUE')
    and DATE_TRUNC(CURRENT_DATE(), MONTH) = SAFE.PARSE_DATE('%d/%m/%Y', recebimentos.mes_referencia)
left join {{ source('planilhas', 'clientes') }} alunos on alunos.id = matriculas.aluno_id
left join {{ source('planilhas', 'clientes') }} responsaveis on alunos.responsavel = responsaveis.id
left join {{ source('planilhas', 'professores') }} professores on professores.id = matriculas.professor_id
left join {{ source('planilhas', 'disciplinas') }} disciplinas on disciplinas.id = matriculas.disciplina_id
left join {{ source('planilhas', 'descontos') }} descontos on descontos.id = matriculas.desconto_id
where matriculas.ativa IN ('true','TRUE') and recebimentos.id is null