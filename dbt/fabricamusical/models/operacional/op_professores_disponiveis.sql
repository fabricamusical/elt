with aulas_pontuais as (
select professor_id, STRING_AGG(data, ', ') as datas, dia_semana, horario 
FROM
(select 
professor_id, 
data, 
dia_semana, 
horario
from {{ source('planilhas', 'experimentais') }} e 
where UPPER(valido) = 'TRUE' AND safe.parse_date('%d/%m/%Y', data) >= current_date()   

UNION ALL

select  
professor_id, 
data_reposicao as data,
dia_semana, 
horario
from {{ source('planilhas', 'reposicoes_prof') }}
where UPPER(valido) = 'TRUE' AND safe.parse_date('%d/%m/%Y', data_reposicao) >= current_date() ) 
group by professor_id, dia_semana, horario
),

aulas_fixas as
(select 
professor_id, 
dia_semana, 
horario
from {{ source('planilhas', 'matriculas') }} m 
where UPPER(m.ativa) = 'TRUE'
)



select 
g.dia as dia, 
g.horario as horario, 
d.nome as disciplina,
STRING_AGG(if(af.professor_id is null, concat(p.apelido, 
if(ap.professor_id is not null, concat(' (exceto: ', ap.datas, ')'), null)), null)
, ' | ') as professores
from {{ source('planilhas', 'grade_horarios') }} g cross join {{ source('planilhas', 'disciplinas') }} d
left join {{ source('planilhas', 'professores_disciplinas') }} pd on d.id = pd.disciplina_id
left join {{ source('planilhas', 'disponibilidade_professores') }} dp on dp.dia = g.dia 
and 
case when dp.horario = 'Dia todo' then true
      when dp.horario = 'Manhã (7h-11h)' then g.horario in ('07:00', '08:00', '09:00', '10:00', '11:00')
      when dp.horario = 'Tarde (12h-17h)' then g.horario in ('12:00', '13:00', '14:00', '15:00', '16:00', '17:00')
      when dp.horario = 'Noite (18h-21h)' then g.horario in ('18:00', '19:00', '20:00', '21:00')
      else dp.horario = g.horario end
left join {{ source('planilhas', 'professores') }} p on pd.professor_id = p.id and dp.professor_id = p.id
left join aulas_pontuais ap on ap.professor_id = p.id and ap.dia_semana = g.dia and ap.horario = g.horario
left join aulas_fixas af on af.professor_id = p.id and af.dia_semana = g.dia and af.horario = g.horario
group by disciplina, dia, horario 
order by disciplina, dia, horario