with
    vendedor as (
        select
            cast (businessentityid as integer) as id_vendedor
            , cast (firstname as string) as nome
            , cast (middlename as string) as nome_meio
            , cast (lastname as string) as sobrenome
        from {{ source('erp', 'person') }}
    )

select *
from vendedor