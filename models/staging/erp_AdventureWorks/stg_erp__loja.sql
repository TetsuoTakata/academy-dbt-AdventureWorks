with 
    loja as (
        select
            cast (businessentityid as integer) as id_loja
            , cast (name as string) as nome_loja
        from {{ source('erp', 'store') }}
    )
select *
from loja