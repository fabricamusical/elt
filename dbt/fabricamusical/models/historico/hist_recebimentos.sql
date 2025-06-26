SELECT * FROM {{ ref('stg_recebimentos')}}
{% if is_incremental() %}
       WHERE last_update > (SELECT max(last_update) FROM {{ this }})
   {% endif %}