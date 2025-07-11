with aulas as (
(select 
c.apelido as aluno, 
professor_id, 
data, 
dia_semana, 
horario, 
sala, 
'EXP' as tipo, 
ROW_NUMBER() OVER (partition by dia_semana, horario, sala order by safe.parse_date('%d/%m/%Y', data) asc) rn 
from {{ source('planilhas', 'experimentais') }} e 
left join {{ source('planilhas', 'clientes') }} c on e.aluno_id = c.id 
where UPPER(valido) = 'TRUE' AND safe.parse_date('%d/%m/%Y', data) >= current_date()   
QUALIFY rn = 1)

UNION ALL

(select 
case 
    when d.tipo = 'Instrumento' then STRING_AGG(c.apelido, ' e ')
    when d.tipo = 'Prática de conjunto' then d.nome 
    else null 
    end as aluno, 
professor_id, 
'null' as data, 
dia_semana, 
horario, 
sala, 
'null' as tipo, 
1 as rn 
from {{ source('planilhas', 'matriculas') }} m 
left join {{ source('planilhas', 'clientes') }} c on m.aluno_id = c.id 
left join {{ source('planilhas', 'disciplinas') }} d on m.disciplina_id = d.id
where UPPER(m.ativa) = 'TRUE'
group by professor_id, data, dia_semana, horario, sala, tipo, m.tipo, m.numero_alunos, d.nome, d.tipo, rn )

UNION ALL

select 
aluno, 
professor_id, 
data_reposicao as data,
dia_semana, 
horario, 
sala, 
'REP' as tipo, 
ROW_NUMBER() OVER (partition by dia_semana, horario, sala order by safe.parse_date('%d/%m/%Y', data_reposicao) asc) rn 
from {{ source('planilhas', 'reposicoes_prof') }}
where UPPER(valido) = 'TRUE' AND safe.parse_date('%d/%m/%Y', data_reposicao) >= current_date()  
QUALIFY rn = 1
)

SELECT g.dia, g.horario, 
STRING_AGG(if(a.sala = 'Sala 1', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala1,
STRING_AGG(if(a.sala = 'Sala 2', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala2,
STRING_AGG(if(a.sala = 'Sala 3', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala3,
STRING_AGG(if(a.sala = 'Sala 4', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala4,
STRING_AGG(if(a.sala = 'Sala 5', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala5,
STRING_AGG(if(a.sala = 'Sala 6', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala6,
STRING_AGG(if(a.sala = 'Sala 7', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala7,
STRING_AGG(if(a.sala = 'Sala 8', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala8,
STRING_AGG(if(a.sala = 'Sala 9', concat(p.apelido, ' - ', aluno,if(a.tipo != 'null', concat(' - ', a.tipo, ': ', data), null)), null), ' | ') as sala9


 FROM {{ source('planilhas', 'grade_horarios') }} g 
 left join aulas a on g.dia = a.dia_semana and a.horario = g.horario 
 left join {{ source('planilhas', 'professores') }} p on a.professor_id = p.id
 group by g.dia, g.horario