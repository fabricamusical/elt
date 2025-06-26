SELECT * FROM {{ ref('stg_matriculas')}}
{% if is_incremental() %}
       WHERE last_update > (SELECT max(last_update) FROM {{ this }})
   {% endif %}