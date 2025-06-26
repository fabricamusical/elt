with aulas as (
(select 
c.apelido as aluno, 
professor_id, 
data, 
dia_semana, 
horario, 
sala, 
'EXP' as tipo, 
ROW_NUMBER() OVER (order by data asc) rn 
from {{ source('planilhas', 'experimentais') }} e 
left join {{ source('planilhas', 'clientes') }} c on e.aluno_id = c.id 
where valido = 'TRUE' AND safe.parse_date('%d/%m/%Y', data) >= current_date()   
QUALIFY rn = 1)

UNION ALL

(select c.apelido as aluno, 
professor_id, 
null as data, 
dia_semana, 
horario, 
sala, 
null as tipo, 
null as rn 
from {{ source('planilhas', 'matriculas') }} m 
left join {{ source('planilhas', 'clientes') }} c on m.aluno_id = c.id 
where ativa = 'TRUE')

UNION ALL

select aluno, 
professor_id, 
data_reposicao as data,
dia_semana, 
horario, 
sala, 
'REP' as tipo, 
ROW_NUMBER() OVER (order by data_reposicao asc) rn 
from {{ source('planilhas', 'reposicoes_prof') }}
where valido = 'TRUE' AND safe.parse_date('%d/%m/%Y', data_reposicao) >= current_date()  
QUALIFY rn = 1
)

SELECT g.dia, g.horario, 
if(a.sala = 'Sala 1', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala1,
if(a.sala = 'Sala 2', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala2,
if(a.sala = 'Sala 3', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala3,
if(a.sala = 'Sala 4', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala4,
if(a.sala = 'Sala 5', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala5,
if(a.sala = 'Sala 6', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala6,
if(a.sala = 'Sala 7', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala7,
if(a.sala = 'Sala 8', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala8,
if(a.sala = 'Sala 9', concat(p.apelido, ' - ', aluno,if(a.tipo is not null, concat(' - ', a.tipo, ': ', data), '')), '') as sala9


 FROM {{ source('planilhas', 'grade_horarios') }} g 
 left join aulas a on g.dia = a.dia_semana and a.horario = g.horario 
 left join {{ source('planilhas', 'professores') }} p on a.professor_id = p.id